//! Task to manage peers and their responses

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use error_fatality::{Nested as _, Split};
use fallible_iterator::{FallibleIterator, IteratorExt};
use futures::{
    StreamExt,
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    stream,
};
use nonempty::NonEmpty;
use sneed::{DbError, EnvError, RwTxn, RwTxnError};
use tokio::task::{self, JoinHandle};
use tokio_stream::StreamNotifyClose;

use super::{
    error::net_task::{self as error, Error},
    mainchain_task::{self, MainchainTaskHandle},
};
use crate::{
    archive::{self, Archive},
    mempool::MemPool,
    net::{
        self, Net, PeerConnectionError, PeerConnectionInfo,
        PeerConnectionMailboxError, PeerConnectionMessage, PeerInfoRx,
        PeerRequest, PeerResponse, PeerStateId, peer_message,
    },
    state::{self, State},
    types::{
        AuthorizedTransaction, BmmResult, Body, Header, Tip,
        net::ResolvedSeedAddress,
        proto::mainchain::{self, Event as MainchainBlockEvent},
    },
    util::{ErrorChain, join_set},
};

const TRANSACTION_RETRY_INTERVAL: Duration = Duration::from_secs(60);

fn transaction_retry_interval() -> impl futures::Stream<Item = ()> {
    let mut interval = tokio::time::interval(TRANSACTION_RETRY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stream::unfold(interval, |mut interval| async move {
        interval.tick().await;
        Some(((), interval))
    })
}

fn transaction_read_error(error: &state::Error) -> bool {
    matches!(
        error,
        state::Error::Db(_)
            | state::Error::BorshSerialize(_)
            | state::Error::Authorization(
                crate::authorization::Error::BorshSerialize(_)
            )
            | state::Error::Amm(state::error::Amm::Db(_))
            | state::Error::BitAsset(state::error::BitAsset::Db(_))
            | state::Error::DutchAuction(state::error::DutchAuction::Db(_))
            | state::Error::ConnectWithdrawalBundleSubmitted(
                state::error::ConnectWithdrawalBundleSubmitted::Db(_)
            )
    )
}

fn relay_pending_transactions(
    env: &sneed::Env<heed::WithoutTls>,
    mempool: &MemPool,
    state: &State,
    net: &Net,
    peer: Option<SocketAddr>,
) -> Result<(), Error> {
    let mut rwtxn = env.write_txn().map_err(EnvError::from)?;
    let pending = mempool.take_all(&rwtxn)?;
    let mut valid = Vec::new();
    for transaction in pending {
        match state.validate_transaction(&rwtxn, &transaction) {
            Ok(_) => valid.push(transaction),
            Err(error) if transaction_read_error(&error) => {
                return Err(error.into());
            }
            Err(error) => {
                let txid = transaction.transaction.txid();
                mempool.delete(&mut rwtxn, txid)?;
                tracing::warn!(%txid, %error, "Delete invalid pending transaction");
            }
        }
    }
    rwtxn.commit().map_err(RwTxnError::from)?;
    let exclude: HashSet<_> = peer
        .map(|peer| {
            net.get_active_peers()
                .into_iter()
                .map(|connected| connected.address)
                .filter(|address| *address != peer)
                .collect()
        })
        .unwrap_or_default();
    for transaction in valid {
        relay_transaction(net, &transaction, exclude.clone())?;
    }
    Ok(())
}

fn relay_transaction(
    net: &Net,
    transaction: &AuthorizedTransaction,
    mut exclude: HashSet<SocketAddr>,
) -> Result<(), Error> {
    loop {
        match net.push_tx(exclude.clone(), transaction) {
            Ok(_) => return Ok(()),
            Err(error @ net::Error::PushTransaction { addr, .. }) => {
                exclude.insert(addr);
                tracing::warn!(%addr, %error, "Exclude the closed peer queue from this relay pass");
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(feature = "zmq")]
#[derive(Debug)]
pub(super) struct ZmqPubHandler {
    pub(super) tx: mpsc::UnboundedSender<zeromq::ZmqMessage>,
    _handle: JoinHandle<()>,
}

#[cfg(feature = "zmq")]
impl ZmqPubHandler {
    // run the handler, obtaining a sender sink and the handler task
    pub async fn new(
        socket_addr: SocketAddr,
    ) -> Result<Self, zeromq::ZmqError> {
        use futures::TryFutureExt as _;
        use zeromq::Socket as _;
        let (tx, rx) = mpsc::unbounded::<zeromq::ZmqMessage>();
        let zmq_pub_addr = format!("tcp://{socket_addr}");
        let mut zmq_pub = zeromq::PubSocket::new();
        let _zmq_endpoint = zmq_pub.bind(&zmq_pub_addr).await?;
        let handle = tokio::task::spawn({
            rx.map(Ok)
                .forward(futures::sink::unfold(
                    zmq_pub,
                    |mut zmq_pub, zmq_msg| async {
                        zeromq::SocketSend::send(&mut zmq_pub, zmq_msg).await?;
                        Ok(zmq_pub)
                    },
                ))
                .unwrap_or_else(|err: zeromq::ZmqError| {
                    tracing::error!("{:#}", ErrorChain::new(&err));
                })
        });
        Ok(Self {
            tx,
            _handle: handle,
        })
    }
}

impl From<net::Error> for Error {
    fn from(err: net::Error) -> Self {
        Self::Net(Box::new(err))
    }
}

fn connect_tip_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    header: &Header,
    body: &Body,
    two_way_peg_data: &mainchain::TwoWayPegData,
) -> Result<(), Error> {
    let block_hash = header.hash();
    if tracing::enabled!(tracing::Level::DEBUG) {
        let merkle_root = header.merkle_root;
        let height = state.try_get_height(rwtxn)?;
        state.apply_block(rwtxn, header, body)?;
        tracing::debug!(?height, %merkle_root, %block_hash, "connected body")
    } else {
        state.apply_block(rwtxn, header, body)?;
    }
    let () = state.connect_two_way_peg_data(rwtxn, two_way_peg_data)?;
    let () = archive.put_header(rwtxn, header)?;
    let () = archive.put_body(rwtxn, block_hash, body)?;
    for transaction in &body.transactions {
        let () = mempool.delete(rwtxn, transaction.txid())?;
    }
    Ok(())
}

pub(in crate::node) fn disconnect_tip_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
) -> Result<(), Error> {
    let tip_block_hash =
        state.try_get_tip(rwtxn)?.ok_or(state::Error::NoTip)?;
    let tip_header = archive.get_header(rwtxn, tip_block_hash)?;
    let tip_body = archive.get_body(rwtxn, tip_block_hash)?;
    let height = state.try_get_height(rwtxn)?.ok_or(state::Error::NoTip)?;
    let two_way_peg_data = {
        let last_applied_deposit_block = state
            .deposit_blocks()
            .rev_iter(rwtxn)
            .map_err(DbError::from)?
            .find_map(|(_, (block_hash, applied_height))| {
                if applied_height < height {
                    Ok(Some((block_hash, applied_height)))
                } else {
                    Ok(None)
                }
            })?;
        let last_applied_withdrawal_bundle_event_block = state
            .withdrawal_bundle_event_blocks()
            .rev_iter(rwtxn)?
            .find_map(|(_, (block_hash, applied_height))| {
                if applied_height < height {
                    Ok(Some((block_hash, applied_height)))
                } else {
                    Ok(None)
                }
            })
            .map_err(DbError::from)?;
        let start_block_hash = match (
            last_applied_deposit_block,
            last_applied_withdrawal_bundle_event_block,
        ) {
            (None, None) => None,
            (Some((block_hash, _)), None) | (None, Some((block_hash, _))) => {
                Some(block_hash)
            }
            (
                Some((deposit_block, deposit_block_applied_height)),
                Some((
                    withdrawal_event_block,
                    withdrawal_event_block_applied_height,
                )),
            ) => {
                match deposit_block_applied_height
                    .cmp(&withdrawal_event_block_applied_height)
                {
                    Ordering::Less => Some(withdrawal_event_block),
                    Ordering::Greater => Some(deposit_block),
                    Ordering::Equal => {
                        if archive.is_main_descendant(
                            rwtxn,
                            withdrawal_event_block,
                            deposit_block,
                        )? {
                            Some(withdrawal_event_block)
                        } else {
                            assert!(archive.is_main_descendant(
                                rwtxn,
                                deposit_block,
                                withdrawal_event_block
                            )?);
                            Some(deposit_block)
                        }
                    }
                }
            }
        };
        let block_infos: Vec<_> = archive
            .main_ancestors(rwtxn, tip_header.prev_main_hash)
            .take_while(|ancestor| {
                Ok(Some(ancestor) != start_block_hash.as_ref())
            })
            .filter_map(|ancestor| {
                let block_info =
                    archive.get_main_block_info(rwtxn, &ancestor)?;
                if block_info.events.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some((ancestor, block_info)))
                }
            })
            .collect()?;
        mainchain::TwoWayPegData {
            block_info: block_infos.into_iter().rev().collect(),
        }
    };
    let () = state.disconnect_two_way_peg_data(rwtxn, &two_way_peg_data)?;
    let () = state.disconnect_tip(rwtxn, &tip_header, &tip_body)?;
    for transaction in tip_body.authorized_transactions().iter().rev() {
        mempool.put(rwtxn, transaction)?;
    }
    Ok(())
}

// a state error means a peer sent an invalid block; it must not be fatal
fn is_fatal_reorg_error(err: &Error) -> bool {
    !matches!(err, Error::State(_))
}

/// Re-org to the specified tip, if it is better than the current tip.
/// The new tip block and all ancestor blocks must exist in the node's archive.
/// A result of `Ok(true)` indicates a successful re-org.
/// A result of `Ok(false)` indicates that no re-org was attempted.
fn reorg_to_tip<Tls>(
    env: &sneed::Env<Tls>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    #[cfg(feature = "zmq")] zmq_pub_handler: &ZmqPubHandler,
    new_tip: Tip,
) -> Result<bool, Error> {
    let mut rwtxn = env.write_txn().map_err(EnvError::from)?;
    let tip_height = state.try_get_height(&rwtxn)?;
    let tip = state
        .try_get_tip(&rwtxn)?
        .map(|tip_hash| {
            let bmm_verification =
                archive.get_best_main_verification(&rwtxn, tip_hash)?;
            Ok::<_, Error>(Tip {
                block_hash: tip_hash,
                main_block_hash: bmm_verification,
            })
        })
        .transpose()?;
    if let Some(tip) = tip {
        // check that new tip is better than current tip
        if archive.better_tip(&rwtxn, tip, new_tip)? != Some(new_tip) {
            tracing::debug!(
                ?tip,
                ?new_tip,
                "New tip is not better than current tip"
            );
            return Ok(false);
        }
    }
    let common_ancestor = if let Some(tip) = tip {
        archive.last_common_ancestor(
            &rwtxn,
            tip.block_hash,
            new_tip.block_hash,
        )?
    } else {
        None
    };
    // Check that all necessary bodies exist before disconnecting tip
    let blocks_to_apply: NonEmpty<(Header, Body)> = {
        let header = archive.get_header(&rwtxn, new_tip.block_hash)?;
        let body = archive.get_body(&rwtxn, new_tip.block_hash)?;
        let ancestors = if let Some(prev_side_hash) = header.prev_side_hash {
            archive
                .ancestors(&rwtxn, prev_side_hash)
                .take_while(|block_hash| {
                    Ok(common_ancestor.is_none_or(|common_ancestor| {
                        *block_hash != common_ancestor
                    }))
                })
                .map(|block_hash| {
                    let header = archive.get_header(&rwtxn, block_hash)?;
                    let body = archive.get_body(&rwtxn, block_hash)?;
                    Ok((header, body))
                })
                .collect()?
        } else {
            Vec::new()
        };
        NonEmpty {
            head: (header, body),
            tail: ancestors,
        }
    };
    // Disconnect tip until common ancestor is reached
    let mut common_ancestor_height = None;
    if let Some(tip_height) = tip_height {
        if let Some(common_ancestor) = common_ancestor {
            common_ancestor_height =
                Some(archive.get_height(&rwtxn, common_ancestor)?);
        }
        tracing::debug!(
            ?tip,
            ?tip_height,
            ?common_ancestor,
            ?common_ancestor_height,
            "Disconnecting tip until common ancestor is reached"
        );
        let disconnects =
            if let Some(common_ancestor_height) = common_ancestor_height {
                tip_height - common_ancestor_height
            } else {
                tip_height + 1
            };
        for _ in 0..disconnects {
            let () = disconnect_tip_(&mut rwtxn, archive, mempool, state)?;
        }
    }
    {
        let tip_hash = state.try_get_tip(&rwtxn)?;
        assert_eq!(tip_hash, common_ancestor);
    }
    let mut two_way_peg_data_batch: Vec<_> = {
        let common_ancestor_header =
            if let Some(common_ancestor) = common_ancestor {
                Some(archive.get_header(&rwtxn, common_ancestor)?)
            } else {
                None
            };
        let common_ancestor_prev_main_hash =
            common_ancestor_header.map(|header| header.prev_main_hash);
        archive
            .main_ancestors(&rwtxn, blocks_to_apply.head.0.prev_main_hash)
            .take_while(|ancestor| {
                Ok(Some(ancestor) != common_ancestor_prev_main_hash.as_ref())
            })
            .map(|ancestor| {
                let block_info =
                    archive.get_main_block_info(&rwtxn, &ancestor)?;
                Ok((ancestor, block_info))
            })
            .collect()?
    };
    // Apply blocks until new tip is reached
    for (header, body) in blocks_to_apply.iter().rev() {
        let two_way_peg_data = {
            let mut two_way_peg_data = mainchain::TwoWayPegData::default();
            'fill_2wpd: while let Some((block_hash, block_info)) =
                two_way_peg_data_batch.pop()
            {
                two_way_peg_data.block_info.replace(block_hash, block_info);
                if block_hash == header.prev_main_hash {
                    break 'fill_2wpd;
                }
            }
            two_way_peg_data
        };
        let () = match connect_tip_(
            &mut rwtxn,
            archive,
            mempool,
            state,
            header,
            body,
            &two_way_peg_data,
        ) {
            Ok(()) => (),
            Err(err) => {
                if !is_fatal_reorg_error(&err) {
                    // The stored body for this block failed validation (e.g. a peer
                    // supplied a body whose contents do not match the header's merkle
                    // root). Abort the reorg and discard the invalid body from the
                    // archive so that the block is reported missing again and the real
                    // body is re-requested, instead of the archive staying poisoned.
                    drop(rwtxn);
                    let mut rwtxn = env.write_txn()?;
                    let () =
                        archive.delete_body(&mut rwtxn, header.hash(), body)?;
                    rwtxn.commit()?;
                }
                return Err(err);
            }
        };
        let new_tip_hash = state.try_get_tip(&rwtxn)?.unwrap();
        let bmm_verification =
            archive.get_best_main_verification(&rwtxn, new_tip_hash)?;
        let new_tip = Tip {
            block_hash: new_tip_hash,
            main_block_hash: bmm_verification,
        };
        if let Some(tip) = tip
            && archive.better_tip(&rwtxn, tip, new_tip)? != Some(new_tip)
        {
            continue;
        }
        rwtxn.commit().map_err(RwTxnError::from)?;
        tracing::info!("synced to tip: {}", new_tip.block_hash);
        rwtxn = env.write_txn().map_err(EnvError::from)?;
    }
    let tip = state.try_get_tip(&rwtxn)?;
    assert_eq!(tip, Some(new_tip.block_hash));
    rwtxn.commit().map_err(RwTxnError::from)?;
    tracing::info!("synced to tip: {}", new_tip.block_hash);
    #[cfg(feature = "zmq")]
    {
        for (idx, (header, _body)) in
            blocks_to_apply.into_iter().rev().enumerate()
        {
            let block_hash = header.hash();
            let height =
                common_ancestor_height.map(|h| h + 1).unwrap_or(0) + idx as u32;
            let mut zmq_msg = zeromq::ZmqMessage::from("hashblock");
            zmq_msg.push_back(bytes::Bytes::copy_from_slice(&block_hash.0));
            zmq_msg.push_back(bytes::Bytes::copy_from_slice(
                &height.to_le_bytes(),
            ));
            zmq_pub_handler.tx.unbounded_send(zmq_msg).unwrap();
        }
    }
    Ok(true)
}

#[derive(Clone)]
struct NetTaskContext {
    env: sneed::Env<heed::WithoutTls>,
    archive: Archive,
    mainchain_task: MainchainTaskHandle,
    mempool: MemPool,
    net: Net,
    state: State,
    #[cfg(feature = "zmq")]
    zmq_pub_handler: Arc<ZmqPubHandler>,
}

/// Message indicating a tip that is ready to reorg to, with the address of the
/// peer connection that caused the request, if it originated from a peer.
/// If the request originates from this node, then the socket address is
/// None.
/// An optional oneshot sender can be used receive the result of attempting
/// to reorg to the new tip, on the corresponding oneshot receiver.
type NewTipReadyMessage =
    (Tip, Option<SocketAddr>, Option<oneshot::Sender<bool>>);

struct NetTask {
    ctxt: NetTaskContext,
    /// Receive a request to forward to the mainchain task, with the address of
    /// the peer connection that caused the request, and the peer state ID of
    /// the request
    forward_mainchain_task_request_rx:
        UnboundedReceiver<(mainchain_task::Request, SocketAddr, PeerStateId)>,
    /// Push a request to forward to the mainchain task, with the address of
    /// the peer connection that caused the request, and the peer state ID of
    /// the request
    forward_mainchain_task_request_tx:
        UnboundedSender<(mainchain_task::Request, SocketAddr, PeerStateId)>,
    mainchain_task_event_rx: UnboundedReceiver<mainchain_task::Event>,
    /// Receive a tip that is ready to reorg to, with the address of the peer
    /// connection that caused the request, if it originated from a peer.
    /// If the request originates from this node, then the socket address is
    /// None.
    /// An optional oneshot sender can be used receive the result of attempting
    /// to reorg to the new tip, on the corresponding oneshot receiver.
    new_tip_ready_rx: UnboundedReceiver<NewTipReadyMessage>,
    /// Push a tip that is ready to reorg to, with the address of the peer
    /// connection that caused the request, if it originated from a peer.
    /// If the request originates from this node, then the socket address is
    /// None.
    /// An optional oneshot sender can be used receive the result of attempting
    /// to reorg to the new tip, on the corresponding oneshot receiver.
    new_tip_ready_tx: UnboundedSender<NewTipReadyMessage>,
    peer_info_rx: PeerInfoRx,
}

impl NetTask {
    fn handle_response(
        ctxt: &NetTaskContext,
        // Attempt to switch to a descendant tip once a body has been
        // stored, if all other ancestor bodies are available.
        // Each descendant tip maps to the peers that sent that tip.
        descendant_tips: &mut HashMap<
            crate::types::BlockHash,
            HashMap<Tip, HashSet<SocketAddr>>,
        >,
        new_tip_ready_tx: &UnboundedSender<NewTipReadyMessage>,
        addr: SocketAddr,
        resp: PeerResponse,
        req: PeerRequest,
    ) -> Result<(), Error> {
        tracing::debug!(?req, ?resp, "starting response handler");
        match (req, resp) {
            (
                PeerRequest::GetBlock(
                    req @ peer_message::GetBlockRequest {
                        block_hash,
                        descendant_tip: Some(descendant_tip),
                        ancestor,
                        peer_state_id: Some(peer_state_id),
                    },
                ),
                ref resp @ PeerResponse::Block {
                    ref header,
                    ref body,
                },
            ) => {
                if header.hash() != block_hash {
                    // Invalid response
                    tracing::warn!(%addr, ?req, ?resp,"Invalid response from peer; unexpected block hash");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok::<_, Error>(());
                }
                {
                    let mut rwtxn =
                        ctxt.env.write_txn().map_err(EnvError::from)?;
                    let () =
                        ctxt.archive.put_body(&mut rwtxn, block_hash, body)?;
                    rwtxn.commit().map_err(RwTxnError::from)?;
                }
                // Notify the peer connection if all requested block bodies are
                // now available
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let ancestor_height = if let Some(ancestor) = ancestor {
                        Some(ctxt.archive.get_height(&rotxn, ancestor)?)
                    } else {
                        None
                    };
                    let earliest_missing_body = ctxt
                        .archive
                        .iter_missing_bodies(
                            &rotxn,
                            block_hash,
                            ancestor_height.map_or(0, |height| height + 1),
                        )
                        .next()?;
                    if let Some(earliest_missing_body) = earliest_missing_body {
                        descendant_tips
                            .entry(earliest_missing_body)
                            .or_default()
                            .entry(descendant_tip)
                            .or_default()
                            .insert(addr);
                    } else {
                        let message = PeerConnectionMessage::BodiesAvailable(
                            peer_state_id,
                        );
                        let _: bool =
                            ctxt.net.push_internal_message(message, addr);
                    }
                }
                // Check if any new tips can be applied,
                // and send new tip ready if so
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let tip = ctxt
                        .state
                        .try_get_tip(&rotxn)?
                        .map(|tip_hash| {
                            let bmm_verification = ctxt
                                .archive
                                .get_best_main_verification(&rotxn, tip_hash)?;
                            Ok::<_, Error>(Tip {
                                block_hash: tip_hash,
                                main_block_hash: bmm_verification,
                            })
                        })
                        .transpose()?;
                    // Find the BMM verification that is an ancestor of
                    // `main_descendant_tip`
                    let main_block_hash = ctxt
                        .archive
                        .get_bmm_results(&rotxn, block_hash)?
                        .into_iter()
                        .map(Result::<_, Error>::Ok)
                        .transpose_into_fallible()
                        .find_map(|(main_block_hash, bmm_result)| {
                            match bmm_result {
                                BmmResult::Failed => Ok(None),
                                BmmResult::Verified => {
                                    if ctxt.archive.is_main_descendant(
                                        &rotxn,
                                        main_block_hash,
                                        descendant_tip.main_block_hash,
                                    )? {
                                        Ok(Some(main_block_hash))
                                    } else {
                                        Ok(None)
                                    }
                                }
                            }
                        })?
                        .unwrap();
                    let block_tip = Tip {
                        block_hash,
                        main_block_hash,
                    };

                    if header.prev_side_hash == tip.map(|tip| tip.block_hash) {
                        tracing::trace!(
                            ?block_tip,
                            origin = %addr,
                            "sending new tip ready"
                        );
                        let () = new_tip_ready_tx
                            .unbounded_send((block_tip, Some(addr), None))
                            .map_err(|err| {
                                Error::SendNewTipReady(err.into_send_error())
                            })?;
                    }
                    let Some(block_descendant_tips) =
                        descendant_tips.remove(&block_hash)
                    else {
                        return Ok(());
                    };
                    for (descendant_tip, sources) in block_descendant_tips {
                        let common_ancestor_height = if let Some(tip) = tip
                            && let Some(common_ancestor) =
                                ctxt.archive.last_common_ancestor(
                                    &rotxn,
                                    descendant_tip.block_hash,
                                    tip.block_hash,
                                )? {
                            Some(
                                ctxt.archive
                                    .get_height(&rotxn, common_ancestor)?,
                            )
                        } else {
                            None
                        };
                        let earliest_missing_body = ctxt
                            .archive
                            .iter_missing_bodies(
                                &rotxn,
                                descendant_tip.block_hash,
                                common_ancestor_height
                                    .map_or(0, |height| height + 1),
                            )
                            .next()?;
                        // If a better tip is ready, send a notification
                        'better_tip: {
                            let next_tip = if let Some(earliest_missing_body) =
                                earliest_missing_body
                            {
                                descendant_tips
                                    .entry(earliest_missing_body)
                                    .or_default()
                                    .entry(descendant_tip)
                                    .or_default()
                                    .extend(sources.iter().cloned());

                                // Parent of the earlist missing body
                                ctxt.archive
                                    .get_header(&rotxn, earliest_missing_body)?
                                    .prev_side_hash
                                    .map(|tip_hash| {
                                        let bmm_verification = ctxt
                                            .archive
                                            .get_best_main_verification(
                                                &rotxn, tip_hash,
                                            )?;
                                        Ok::<_, Error>(Tip {
                                            block_hash: tip_hash,
                                            main_block_hash: bmm_verification,
                                        })
                                    })
                                    .transpose()?
                            } else {
                                Some(descendant_tip)
                            };
                            let Some(next_tip) = next_tip else {
                                break 'better_tip;
                            };
                            if let Some(tip) = tip
                                && ctxt
                                    .archive
                                    .better_tip(&rotxn, tip, next_tip)?
                                    != Some(next_tip)
                            {
                                break 'better_tip;
                            } else {
                                tracing::debug!(
                                    new_tip = ?next_tip,
                                    "sending new tip ready to sources"
                                );
                                for addr in sources {
                                    tracing::trace!(%addr, new_tip = ?next_tip, "sending new tip ready");
                                    let () = new_tip_ready_tx
                                        .unbounded_send((
                                            next_tip,
                                            Some(addr),
                                            None,
                                        ))
                                        .map_err(|err| {
                                            Error::SendNewTipReady(
                                                err.into_send_error(),
                                            )
                                        })?;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
            (
                PeerRequest::GetBlock(peer_message::GetBlockRequest {
                    block_hash: req_block_hash,
                    descendant_tip: Some(_),
                    ancestor: _,
                    peer_state_id: Some(_),
                }),
                PeerResponse::NoBlock {
                    block_hash: resp_block_hash,
                },
            ) if req_block_hash == resp_block_hash => Ok(()),
            (
                PeerRequest::GetHeaders(
                    ref req @ peer_message::GetHeadersRequest {
                        ref start,
                        end,
                        height: Some(height),
                        peer_state_id: Some(peer_state_id),
                    },
                ),
                PeerResponse::Headers(headers),
            ) => {
                // check that the end header is as requested
                let Some(end_header) = headers.last() else {
                    tracing::warn!(%addr, ?req, "Invalid response from peer; missing end header");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                };
                let end_header_hash = end_header.hash();
                if end_header_hash != end {
                    tracing::warn!(%addr, ?req, ?end_header,"Invalid response from peer; unexpected end header");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // Must be at least one header due to previous check
                let start_hash = headers.first().unwrap().prev_side_hash;
                // check that the first header is after a start block
                if let Some(start_hash) = start_hash
                    && !start.contains(&start_hash)
                {
                    tracing::warn!(%addr, ?req, %start_hash, "Invalid response from peer; invalid start hash");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // check that the end header height is as expected
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let start_height = if let Some(start_hash) = start_hash {
                        Some(ctxt.archive.get_height(&rotxn, start_hash)?)
                    } else {
                        None
                    };
                    let end_height = match start_height {
                        Some(start_height) => {
                            start_height + headers.len() as u32
                        }
                        None => headers.len() as u32 - 1,
                    };
                    if end_height != height {
                        tracing::warn!(%addr, ?req, ?start_hash, "Invalid response from peer; invalid end height");
                        let () = ctxt.net.remove_active_peer(addr);
                        return Ok(());
                    }
                }
                // check that headers are sequential based on prev_side_hash,
                // and that no header is an invalidated block.
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let mut prev_side_hash = start_hash;
                    for header in &headers {
                        if header.prev_side_hash != prev_side_hash {
                            tracing::warn!(%addr, ?req, ?headers,"Invalid response from peer; non-sequential headers");
                            let () = ctxt.net.remove_active_peer(addr);
                            return Ok(());
                        }
                        if ctxt
                            .archive
                            .invalidated_block(&rotxn, &header.hash())?
                        {
                            tracing::warn!(%addr, ?req, ?headers,"Invalid response from peer; invalidated block header");
                            let () = ctxt.net.remove_active_peer(addr);
                            return Ok(());
                        }
                        prev_side_hash = Some(header.hash());
                    }
                }
                // Store new headers
                let () = tokio::task::block_in_place(|| {
                    let mut rwtxn =
                        ctxt.env.write_txn().map_err(EnvError::from)?;
                    for header in &headers {
                        let block_hash = header.hash();
                        if ctxt
                            .archive
                            .try_get_header(&rwtxn, block_hash)?
                            .is_none()
                        {
                            if let Some(parent) = header.prev_side_hash
                                && ctxt
                                    .archive
                                    .try_get_header(&rwtxn, parent)?
                                    .is_none()
                            {
                                break;
                            } else {
                                ctxt.archive.put_header(&mut rwtxn, header)?;
                            }
                        }
                    }
                    rwtxn.commit().map_err(RwTxnError::from)?;
                    Ok::<_, Error>(())
                })?;
                // Notify peer connection that headers are available
                let message = PeerConnectionMessage::Headers(peer_state_id);
                let _: bool = ctxt.net.push_internal_message(message, addr);
                Ok(())
            }
            (
                PeerRequest::GetHeaders(peer_message::GetHeadersRequest {
                    start: _,
                    end,
                    height: _,
                    peer_state_id: _,
                }),
                PeerResponse::NoHeader { block_hash },
            ) if end == block_hash => Ok(()),
            (
                PeerRequest::PushTransaction(
                    peer_message::PushTransactionRequest { transaction: _ },
                ),
                PeerResponse::TransactionAccepted(_),
            ) => Ok(()),
            (
                PeerRequest::PushTransaction(
                    peer_message::PushTransactionRequest { transaction: _ },
                ),
                PeerResponse::TransactionRejected(_),
            ) => Ok(()),
            (
                req @ (PeerRequest::GetBlock { .. }
                | PeerRequest::GetHeaders { .. }
                | PeerRequest::PushTransaction { .. }),
                resp,
            ) => {
                // Invalid response
                tracing::warn!(%addr, ?req, ?resp,"Invalid response from peer");
                let () = ctxt.net.remove_active_peer(addr);
                Ok(())
            }
        }
    }

    fn handle_mainchain_block_event(
        ctxt: &NetTaskContext,
        _event: MainchainBlockEvent,
    ) -> Result<(), Error> {
        let mut rwtxn = ctxt.env.write_txn().map_err(EnvError::from)?;
        while let Some(state_tip) = ctxt.state.try_get_tip(&rwtxn)?
            && !ctxt
                .archive
                .side_tips()
                .sidechain_tips()
                .contains_key(&rwtxn, &state_tip)
                .map_err(archive::Error::from)?
        {
            let header = ctxt.archive.get_header(&rwtxn, state_tip)?;
            let body = ctxt.archive.get_body(&rwtxn, state_tip)?;
            let () = ctxt.state.disconnect_tip(&mut rwtxn, &header, &body)?;
        }
        let best_side_tip = ctxt
            .archive
            .side_tips()
            .best_side_tip(&rwtxn)
            .map_err(archive::Error::from)?;
        rwtxn.commit()?;
        if let Some(best_side_tip) = best_side_tip {
            let best_side_tip = Tip {
                block_hash: best_side_tip.block_hash,
                main_block_hash: best_side_tip.info.main_block_hash,
            };
            let _: bool = reorg_to_tip(
                &ctxt.env,
                &ctxt.archive,
                &ctxt.mempool,
                &ctxt.state,
                #[cfg(feature = "zmq")]
                &ctxt.zmq_pub_handler,
                best_side_tip,
            )?;
        }
        Ok(())
    }

    fn handle_mainchain_task_response(
        ctxt: &NetTaskContext,
        mainchain_task_request_sources: &mut HashMap<
            mainchain_task::Request,
            HashSet<(SocketAddr, PeerStateId)>,
        >,
        response: mainchain_task::Response,
    ) -> Result<(), Error> {
        let request = (&response).into();
        match response {
            mainchain_task::Response::AncestorInfos(block_hash, res) => {
                let Some(sources) =
                    mainchain_task_request_sources.remove(&request)
                else {
                    return Ok(());
                };
                let res = res.map_err(Arc::new);
                for (addr, peer_state_id) in sources {
                    let message = match res {
                        Ok(true) => PeerConnectionMessage::MainchainAncestors(
                            peer_state_id,
                        ),
                        Ok(false) => {
                            PeerConnectionMessage::MainchainAncestorsError(
                                error::MainchainAncestors::BlockNotAvailable {
                                    block_hash,
                                },
                            )
                        }
                        Err(ref err) => {
                            PeerConnectionMessage::MainchainAncestorsError(
                                err.clone().into(),
                            )
                        }
                    };
                    let _: bool = ctxt.net.push_internal_message(message, addr);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn handle_mainchain_task_event(
        ctxt: &NetTaskContext,
        mainchain_task_request_sources: &mut HashMap<
            mainchain_task::Request,
            HashSet<(SocketAddr, PeerStateId)>,
        >,
        event: mainchain_task::Event,
    ) -> Result<(), Error> {
        match event {
            mainchain_task::Event::Block(event) => {
                Self::handle_mainchain_block_event(ctxt, event)
            }
            mainchain_task::Event::Response(resp) => {
                Self::handle_mainchain_task_response(
                    ctxt,
                    mainchain_task_request_sources,
                    resp,
                )
            }
        }
    }

    async fn run(self) -> Result<(), Error> {
        tracing::debug!("starting net task");
        #[derive(Debug)]
        enum MailboxItem {
            AcceptConnection(
                Result<
                    Option<SocketAddr>,
                    <net::error::AcceptConnection as Split>::Fatal,
                >,
            ),
            // Forward a mainchain task request, along with the peer that
            // caused the request, and the peer state ID of the request
            ForwardMainchainTaskRequest(
                mainchain_task::Request,
                SocketAddr,
                PeerStateId,
            ),
            MainchainTaskEvent(mainchain_task::Event),
            // Apply new tip from peer or self.
            // An optional oneshot sender can be used receive the result of
            // attempting to reorg to the new tip, on the corresponding oneshot
            // receiver.
            NewTipReady(Tip, Option<SocketAddr>, Option<oneshot::Sender<bool>>),
            PeerInfo(Option<(SocketAddr, Option<PeerConnectionInfo>)>),
            // Signal to reconnect to a peer
            ReconnectPeer(ResolvedSeedAddress),
            // The loop that dials known peers stopped on an error
            RedialKnownPeers(Box<net::Error>),
            RetryTransactions,
        }
        let accept_connections = stream::try_unfold((), |()| {
            let env = self.ctxt.env.clone();
            let net = self.ctxt.net.clone();
            let fut = async move {
                let maybe_socket_addr =
                    net.accept_incoming(env).await.into_nested()?;
                // / Return:
                // - The value to yield (maybe_socket_addr)
                // - The state for the next iteration (())
                // Wrapped in Result and Option
                Result::<_, _>::Ok(Some((maybe_socket_addr, ())))
            };
            Box::pin(fut)
        })
        .filter_map(async |item| match item {
            Ok(Ok(maybe_socket_addr)) => Some(Ok(maybe_socket_addr)),
            Ok(Err(non_fatal_err)) => {
                // type the error explicitly
                let non_fatal_err:
                    <net::error::AcceptConnection as Split>::Jfyi =
                    non_fatal_err;
                tracing::error!(
                    "Failed to accept connection: {:#}",
                    ErrorChain::new(&non_fatal_err)
                );
                None
            }
            Err(fatal_err) => Some(Err(fatal_err)),
        })
        .map(MailboxItem::AcceptConnection);
        let forward_request_stream = self
            .forward_mainchain_task_request_rx
            .map(|(request, addr, peer_state_id)| {
                MailboxItem::ForwardMainchainTaskRequest(
                    request,
                    addr,
                    peer_state_id,
                )
            });
        let mainchain_task_event_stream = self
            .mainchain_task_event_rx
            .map(MailboxItem::MainchainTaskEvent);
        let new_tip_ready_stream =
            self.new_tip_ready_rx.map(|(block_hash, addr, resp_tx)| {
                MailboxItem::NewTipReady(block_hash, addr, resp_tx)
            });
        let peer_info_stream = StreamNotifyClose::new(self.peer_info_rx)
            .map(MailboxItem::PeerInfo);
        let (reconnect_peer_spawner, reconnect_peer_rx) = join_set::new();
        let reconnect_peer_stream = reconnect_peer_rx
            .map(|addr| MailboxItem::ReconnectPeer(addr.unwrap()));
        let redial_known_peers_stream = {
            const MIN_DELAY: Duration = Duration::from_secs(60);
            const MAX_DELAY: Duration = Duration::from_secs(600);
            let env = self.ctxt.env.clone();
            let net = self.ctxt.net.clone();
            stream::once(async move {
                net.redial_known_peers(env, MIN_DELAY, MAX_DELAY).await
            })
            .filter_map(async |res| res.err().map(Box::new))
            .map(MailboxItem::RedialKnownPeers)
        };
        let retry_transactions = transaction_retry_interval()
            .map(|()| MailboxItem::RetryTransactions);
        let mut mailbox_stream = stream::select_all([
            accept_connections.boxed(),
            forward_request_stream.boxed(),
            mainchain_task_event_stream.boxed(),
            new_tip_ready_stream.boxed(),
            peer_info_stream.boxed(),
            reconnect_peer_stream.boxed(),
            redial_known_peers_stream.boxed(),
            retry_transactions.boxed(),
        ]);
        // Attempt to switch to a descendant tip once a body has been
        // stored, if all other ancestor bodies are available.
        // Each descendant tip maps to the peers that sent that tip.
        let mut descendant_tips = HashMap::<
            crate::types::BlockHash,
            HashMap<Tip, HashSet<SocketAddr>>,
        >::new();
        // Map associating mainchain task requests with the peer(s) that
        // caused the request, and the request peer state ID
        let mut mainchain_task_request_sources = HashMap::<
            mainchain_task::Request,
            HashSet<(SocketAddr, PeerStateId)>,
        >::new();
        while let Some(mailbox_item) = mailbox_stream.next().await {
            tracing::trace!(?mailbox_item, "received new mailbox item");
            match mailbox_item {
                MailboxItem::AcceptConnection(res) => match res {
                    // We received a connection new incoming network connection, but no peer
                    // was added
                    Ok(None) => {
                        continue;
                    }
                    Ok(Some(addr)) => {
                        tracing::trace!(%addr, "accepted new incoming connection");
                    }
                    Err(fatal_err) => {
                        // explicitly type error
                        let fatal_err: <net::error::AcceptConnection as Split>::Fatal =
                            fatal_err;
                        tracing::error!(
                            "failed to accept connection: {:#}",
                            ErrorChain::new(&fatal_err)
                        );
                    }
                },
                MailboxItem::ForwardMainchainTaskRequest(
                    request,
                    peer,
                    peer_state_id,
                ) => {
                    if self.ctxt.mainchain_task.request(request).is_err() {
                        tracing::warn!(
                            ?request,
                            %peer,
                            "the mainchain task took no request"
                        );
                        continue;
                    }
                    mainchain_task_request_sources
                        .entry(request)
                        .or_default()
                        .insert((peer, peer_state_id));
                }
                MailboxItem::MainchainTaskEvent(event) => {
                    let () = Self::handle_mainchain_task_event(
                        &self.ctxt,
                        &mut mainchain_task_request_sources,
                        event,
                    )?;
                }
                MailboxItem::NewTipReady(new_tip, addr, resp_tx) => {
                    let reorg_result = task::block_in_place(|| {
                        {
                            let rotxn = self
                                .ctxt
                                .env
                                .read_txn()
                                .map_err(|err| Error::DbEnv(err.into()))?;
                            if !self
                                .ctxt
                                .archive
                                .side_tips()
                                .sidechain_tips()
                                .contains_key(&rotxn, &new_tip.block_hash)
                                .map_err(archive::Error::from)?
                            {
                                return Ok(false);
                            }
                            let side_tips_tip = self
                                .ctxt
                                .archive
                                .side_tips()
                                .get_mainchain_tip(&rotxn)
                                .map_err(archive::Error::from)?;
                            if !self.ctxt.archive.is_main_descendant(
                                &rotxn,
                                new_tip.main_block_hash,
                                side_tips_tip.block_hash(),
                            )? {
                                return Ok(false);
                            }
                        }
                        reorg_to_tip(
                            &self.ctxt.env,
                            &self.ctxt.archive,
                            &self.ctxt.mempool,
                            &self.ctxt.state,
                            #[cfg(feature = "zmq")]
                            &self.ctxt.zmq_pub_handler,
                            new_tip,
                        )
                    });
                    let reorg_applied = match reorg_result {
                        Ok(applied) => applied,
                        Err(err) if is_fatal_reorg_error(&err) => {
                            return Err(err);
                        }
                        // an invalid block must not kill the net task; drop the
                        // peer and keep running
                        Err(err) => {
                            tracing::warn!(
                                ?new_tip,
                                ?addr,
                                err = format!("{:#}", ErrorChain::new(&err)),
                                "rejecting invalid tip from peer"
                            );
                            if let Some(addr) = addr {
                                let () = self.ctxt.net.remove_active_peer(addr);
                            }
                            false
                        }
                    };
                    if let Some(resp_tx) = resp_tx {
                        let () = resp_tx
                            .send(reorg_applied)
                            .map_err(|_| Error::SendReorgResultOneshot)?;
                    }
                }
                MailboxItem::PeerInfo(None) => {
                    return Err(Error::PeerInfoRxClosed);
                }
                MailboxItem::PeerInfo(Some((addr, None))) => {
                    // peer connection is closed, remove it
                    tracing::warn!(%addr, "Connection to peer closed");
                    let () = self.ctxt.net.remove_active_peer(addr);
                    continue;
                }
                MailboxItem::PeerInfo(Some((addr, Some(peer_info)))) => {
                    const RECONNECT_DELAY: Duration = Duration::from_secs(10);
                    tracing::trace!(%addr, ?peer_info, "mailbox item: received PeerInfo");
                    match peer_info {
                        PeerConnectionInfo::Connected => {
                            relay_pending_transactions(
                                &self.ctxt.env,
                                &self.ctxt.mempool,
                                &self.ctxt.state,
                                &self.ctxt.net,
                                Some(addr),
                            )?;
                        }
                        PeerConnectionInfo::Error {
                            err:
                                PeerConnectionError::Mailbox(
                                    PeerConnectionMailboxError::HeartbeatTimeout,
                                ),
                            resolved_addr,
                        } => {
                            // Attempt to reconnect if a valid message was
                            // received successfully
                            let Some(received_msg_successfully) =
                                self.ctxt.net.try_with_active_peer_connection(
                                    addr,
                                    |conn_handle| {
                                        conn_handle.received_msg_successfully()
                                    },
                                )
                            else {
                                continue;
                            };
                            let () = self.ctxt.net.remove_active_peer(addr);
                            let reconnect_addr = if received_msg_successfully {
                                resolved_addr
                            } else if let (_, Some(next_addr)) =
                                resolved_addr.pop_first_ip_addr()
                            {
                                next_addr
                            } else {
                                continue;
                            };
                            reconnect_peer_spawner.spawn(async move {
                                tokio::time::sleep(RECONNECT_DELAY).await;
                                reconnect_addr
                            });
                        }
                        PeerConnectionInfo::Error { err, resolved_addr } => {
                            let bad_magic = err.is_bad_magic();
                            let retry_connection = err
                                .is_duplicate_connection()
                                || err.is_connect_timeout();
                            let err_msg =
                                format!("{:#}", ErrorChain::new(&err));
                            tracing::error!(%addr, err = err_msg, "Peer connection error");
                            let () = self.ctxt.net.remove_active_peer(addr);
                            if retry_connection {
                                reconnect_peer_spawner.spawn(async move {
                                    tokio::time::sleep(RECONNECT_DELAY).await;
                                    resolved_addr
                                });
                            } else if !bad_magic
                                && let (_, Some(next_addr)) =
                                    resolved_addr.pop_first_ip_addr()
                            {
                                reconnect_peer_spawner.spawn(async move {
                                    tokio::time::sleep(RECONNECT_DELAY).await;
                                    next_addr
                                });
                            }
                            // A peer on another network never becomes useful,
                            // so it must not survive into the next start.
                            if bad_magic {
                                let mut rwtxn = self
                                    .ctxt
                                    .env
                                    .write_txn()
                                    .map_err(EnvError::from)?;
                                let forgotten = self
                                    .ctxt
                                    .net
                                    .forget_peer(&mut rwtxn, &addr)?;
                                rwtxn.commit().map_err(RwTxnError::from)?;
                                if forgotten {
                                    tracing::warn!(
                                        %addr,
                                        "forgot peer: it runs another network"
                                    );
                                }
                            }
                        }
                        PeerConnectionInfo::NeedMainchainAncestors {
                            main_hash,
                            peer_state_id,
                        } => {
                            let request =
                                mainchain_task::Request::AncestorInfos(
                                    main_hash,
                                );
                            let () = self
                                .forward_mainchain_task_request_tx
                                .unbounded_send((request, addr, peer_state_id))
                                .map_err(|_| {
                                    Error::ForwardMainchainTaskRequest
                                })?;
                        }
                        PeerConnectionInfo::NewTipReady(new_tip) => {
                            tracing::debug!(
                                ?new_tip,
                                %addr,
                                "mailbox item: received NewTipReady from peer, sending on channel"
                            );
                            self.new_tip_ready_tx
                                .unbounded_send((new_tip, Some(addr), None))
                                .map_err(|err| {
                                    Error::SendNewTipReady(
                                        err.into_send_error(),
                                    )
                                })?;
                        }
                        PeerConnectionInfo::NewTransaction(new_tx) => {
                            let mut rwtxn = self
                                .ctxt
                                .env
                                .write_txn()
                                .map_err(EnvError::from)?;
                            if self.ctxt.mempool.transactions
                                .try_get(&rwtxn, &new_tx.transaction.txid())
                                .map_err(DbError::from)?.is_some()
                            {
                                continue;
                            }
                            match self.ctxt.mempool.put(&mut rwtxn, &new_tx) {
                                Ok(()) => (),
                                Err(crate::mempool::Error::UtxoDoubleSpent) => {
                                    tracing::debug!(
                                        %addr,
                                        txid = %new_tx.transaction.txid(),
                                        "Reject peer transaction: UTXO already spent"
                                    );
                                    continue;
                                }
                                Err(err) => return Err(err.into()),
                            }
                            rwtxn.commit().map_err(RwTxnError::from)?;
                            relay_transaction(
                                &self.ctxt.net,
                                &new_tx,
                                HashSet::from_iter([addr]),
                            )?;
                        }
                        PeerConnectionInfo::Response(boxed) => {
                            let (resp, req) = *boxed;
                            tracing::trace!(
                                resp = format!("{resp:#?}"),
                                req = format!("{req:#?}"),
                                "mail box: received PeerConnectionInfo::Response"
                            );
                            let () = tokio::task::block_in_place(|| {
                                Self::handle_response(
                                    &self.ctxt,
                                    &mut descendant_tips,
                                    &self.new_tip_ready_tx,
                                    addr,
                                    resp,
                                    req,
                                )
                            })?;
                        }
                    }
                }
                MailboxItem::ReconnectPeer(resolved_addr) => {
                    let peer_address =
                        resolved_addr.as_seed_address().to_owned();
                    match self
                        .ctxt
                        .net
                        .connect_peer(self.ctxt.env.clone(), resolved_addr)
                    {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::error!(
                                %peer_address,
                                "Failed to connect to peer: {:#}",
                                ErrorChain::new(&err)
                            )
                        }
                    }
                }
                MailboxItem::RedialKnownPeers(err) => {
                    return Err(Error::Net(err));
                }
                MailboxItem::RetryTransactions => {
                    relay_pending_transactions(
                        &self.ctxt.env,
                        &self.ctxt.mempool,
                        &self.ctxt.state,
                        &self.ctxt.net,
                        None,
                    )?;
                }
            }
        }
        Ok(())
    }
}

/// Handle to the net task.
/// Task is aborted on drop.
#[derive(Clone)]
pub(super) struct NetTaskHandle {
    task: Arc<JoinHandle<()>>,
    /// Push a tip that is ready to reorg to, with the address of the peer
    /// connection that caused the request, if it originated from a peer.
    /// If the request originates from this node, then the socket address is
    /// None.
    /// An optional oneshot sender can be used receive the result of attempting
    /// to reorg to the new tip, on the corresponding oneshot receiver.
    new_tip_ready_tx: UnboundedSender<NewTipReadyMessage>,
}

impl NetTaskHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: &tokio::runtime::Runtime,
        env: sneed::Env<heed::WithoutTls>,
        archive: Archive,
        mainchain_task: MainchainTaskHandle,
        mainchain_task_event_rx: UnboundedReceiver<mainchain_task::Event>,
        mempool: MemPool,
        net: Net,
        peer_info_rx: PeerInfoRx,
        state: State,
        #[cfg(feature = "zmq")] zmq_pub_handler: Arc<ZmqPubHandler>,
    ) -> Self {
        let ctxt = NetTaskContext {
            env,
            archive,
            mainchain_task,
            mempool,
            net,
            state,
            #[cfg(feature = "zmq")]
            zmq_pub_handler,
        };
        let (
            forward_mainchain_task_request_tx,
            forward_mainchain_task_request_rx,
        ) = mpsc::unbounded();
        let (new_tip_ready_tx, new_tip_ready_rx) = mpsc::unbounded();
        let task = NetTask {
            ctxt,
            forward_mainchain_task_request_tx,
            forward_mainchain_task_request_rx,
            mainchain_task_event_rx,
            new_tip_ready_tx: new_tip_ready_tx.clone(),
            new_tip_ready_rx,
            peer_info_rx,
        };
        let task = runtime.spawn(async {
            if let Err(err) = task.run().await {
                tracing::error!("Net task error: {:#}", ErrorChain::new(&err));
            }
        });
        NetTaskHandle {
            task: Arc::new(task),
            new_tip_ready_tx,
        }
    }

    /// Push a tip that is ready to reorg to, and await successful application.
    /// A result of Ok(true) indicates that the tip was applied and reorged
    /// to successfully.
    /// A result of Ok(false) indicates that the tip was not reorged to.
    pub async fn new_tip_ready_confirm(
        &self,
        new_tip: Tip,
    ) -> Result<bool, Error> {
        tracing::debug!(?new_tip, "sending new tip ready confirm");
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let () = self
            .new_tip_ready_tx
            .unbounded_send((new_tip, None, Some(oneshot_tx)))
            .map_err(|err| Error::SendNewTipReady(err.into_send_error()))?;
        oneshot_rx.await.map_err(Error::ReceiveReorgResultOneshot)
    }
}

impl Drop for NetTaskHandle {
    // If only one reference exists (ie. within self), abort the net task.
    fn drop(&mut self) {
        // use `Arc::get_mut` since `Arc::into_inner` requires ownership of the
        // Arc, and cloning would increase the reference count
        if let Some(task) = Arc::get_mut(&mut self.task) {
            tracing::debug!("dropping net task handle, aborting task");
            task.abort()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        node::net_task::{Error, is_fatal_reorg_error},
        state,
    };

    // a peer's invalid block (value out > value in) must not be fatal
    #[test]
    fn invalid_peer_block_is_not_fatal() {
        let err = Error::State(Box::new(state::Error::NotEnoughFees));
        assert!(!is_fatal_reorg_error(&err));
    }

    // local infrastructure errors stay fatal
    #[test]
    fn infrastructure_error_is_fatal() {
        assert!(is_fatal_reorg_error(&Error::PeerInfoRxClosed));
    }
}

#[cfg(test)]
mod peer_retry_test {
    use std::{net::Ipv4Addr, time::Duration};

    use anyhow::Context;

    use crate::net::PeerConnectionStatus;
    use crate::{
        net::make_server_endpoint,
        node::Node,
        types::{Network, proto::mainchain::ValidatorClient},
    };

    async fn temp_node(
        runtime: &tokio::runtime::Runtime,
    ) -> anyhow::Result<(temp_dir::TempDir, Node)> {
        let temp_dir = temp_dir::TempDir::new()?;
        let channel =
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
                .connect_lazy();
        let node = Node::new(
            (Ipv4Addr::LOCALHOST, 0).into(),
            temp_dir.path(),
            None,
            Network::Regtest,
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            ValidatorClient::new(channel),
            None,
            runtime,
            #[cfg(feature = "zmq")]
            (Ipv4Addr::LOCALHOST, 0).into(),
        )
        .await?;
        Ok((temp_dir, node))
    }

    fn signed_transaction(
        node: &Node,
        path: &std::path::Path,
    ) -> anyhow::Result<crate::types::AuthorizedTransaction> {
        use crate::types::{FilledOutput, OutPoint, OutPointKey, Transaction};
        use heed::types::SerdeBincode;
        use sneed::DatabaseUnique;
        let wallet = crate::wallet::Wallet::new(&path.join("wallet"))?;
        wallet.set_seed(&[2; 64])?;
        let address = wallet.get_new_address()?;
        let outpoint = OutPoint::Regular {
            txid: [3; 32].into(),
            vout: 0,
        };
        let output = FilledOutput::new_bitcoin_value(
            address,
            bitcoin::Amount::from_sat(1_000),
        );
        let mut rwtxn = node.env.write_txn()?;
        let utxos =
            DatabaseUnique::<OutPointKey, SerdeBincode<FilledOutput>>::create(
                &node.env, &mut rwtxn, "utxos",
            )?;
        utxos.put(&mut rwtxn, &OutPointKey::from(&outpoint), &output)?;
        rwtxn.commit()?;
        wallet.put_utxos(&std::collections::HashMap::from([(
            outpoint, output,
        )]))?;
        let transaction = Transaction::new(
            vec![outpoint],
            vec![
                FilledOutput::new_bitcoin_value(
                    address,
                    bitcoin::Amount::from_sat(900),
                )
                .into(),
            ],
        );
        Ok(wallet.authorize(transaction)?)
    }

    async fn wait_for_transaction(
        node: &Node,
        txid: crate::types::Txid,
    ) -> anyhow::Result<crate::types::AuthorizedTransaction> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(transaction) =
                    node.get_authorized_transaction(txid)?
                {
                    return Ok(transaction);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("The peer did not receive the transaction")?
    }

    fn test_net_task(
        node: &Node,
        peer_info_rx: crate::net::PeerInfoRx,
    ) -> super::NetTask {
        use futures::channel::mpsc;
        let (
            forward_mainchain_task_request_tx,
            forward_mainchain_task_request_rx,
        ) = mpsc::unbounded();
        let (_mainchain_task_event_tx, mainchain_task_event_rx) =
            mpsc::unbounded();
        let (new_tip_ready_tx, new_tip_ready_rx) = mpsc::unbounded();
        super::NetTask {
            ctxt: super::NetTaskContext {
                env: node.env.clone(),
                archive: node.archive.clone(),
                mainchain_task: node.mainchain_task.clone(),
                mempool: node.mempool.clone(),
                net: node.net.clone(),
                state: node.state.clone(),
                #[cfg(feature = "zmq")]
                zmq_pub_handler: node.zmq_pub_handler.clone(),
            },
            forward_mainchain_task_request_tx,
            forward_mainchain_task_request_rx,
            mainchain_task_event_rx,
            new_tip_ready_tx,
            new_tip_ready_rx,
            peer_info_rx,
        }
    }

    #[test]
    fn duplicate_submission_preserves_signed_transaction() -> anyhow::Result<()>
    {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (temp_dir, node) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&node, temp_dir.path())?;
            let txid = transaction.transaction.txid();
            let initial = node.broadcast_transaction(&transaction)?;
            let duplicate = node.broadcast_transaction(&transaction)?;
            assert_eq!(initial.txid, txid);
            assert_eq!(initial.peer_count, 0);
            assert_eq!(duplicate.peer_count, 0);
            node.submit_transaction(&transaction)?;
            let exported = node
                .get_authorized_transaction(txid)?
                .context("The signed transaction is absent")?;
            assert_eq!(
                bincode::serialize(&exported)?,
                bincode::serialize(&transaction)?
            );
            assert_eq!(node.get_all_transactions()?.len(), 1);
            assert!(node.get_authorized_transaction([9; 32].into())?.is_none());
            assert!(node.rebroadcast_transaction([9; 32].into()).is_err());
            let mut invalid = transaction.clone();
            invalid.authorizations.clear();
            assert!(node.broadcast_transaction(&invalid).is_err());
            let exported = node
                .get_authorized_transaction(txid)?
                .context("The signed transaction is absent")?;
            assert_eq!(
                bincode::serialize(&exported)?,
                bincode::serialize(&transaction)?
            );
            Ok(())
        })
    }

    #[test]
    fn broadcast_rejects_input_conflicts() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (temp_dir, node) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&node, temp_dir.path())?;
            let wallet =
                crate::wallet::Wallet::new(&temp_dir.path().join("wallet"))?;
            node.submit_transaction(&transaction)?;
            let mut conflict = transaction.transaction.clone();
            conflict.outputs[0].memo = vec![1];
            let conflict = wallet.authorize(conflict)?;
            let result = node.broadcast_transaction(&conflict);
            assert!(matches!(
                result,
                Err(crate::node::Error::MemPool(
                    crate::mempool::Error::UtxoDoubleSpent
                ))
            ));
            let mut repeated = transaction.transaction.clone();
            repeated.inputs.push(repeated.inputs[0]);
            let repeated = wallet.authorize(repeated)?;
            assert!(node.broadcast_transaction(&repeated).is_err());
            assert_eq!(node.get_all_transactions()?.len(), 1);
            Ok(())
        })
    }

    #[test]
    fn pending_transaction_reaches_a_new_peer() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (source_dir, source) = temp_node(&runtime).await?;
            let (peer_dir, peer) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&source, source_dir.path())?;
            signed_transaction(&peer, peer_dir.path())?;
            let txid = transaction.transaction.txid();
            assert_eq!(
                source.broadcast_transaction(&transaction)?.peer_count,
                0
            );
            source.connect_peer(peer.net.server.local_addr()?)?;
            let received = wait_for_transaction(&peer, txid).await?;
            assert_eq!(
                bincode::serialize(&received)?,
                bincode::serialize(&transaction)?
            );
            let mut rwtxn = peer.env.write_txn()?;
            peer.mempool.delete(&mut rwtxn, txid)?;
            rwtxn.commit()?;
            super::relay_pending_transactions(
                &source.env,
                &source.mempool,
                &source.state,
                &source.net,
                None,
            )?;
            let received = wait_for_transaction(&peer, txid).await?;
            assert_eq!(
                bincode::serialize(&received)?,
                bincode::serialize(&transaction)?
            );
            let mut rwtxn = peer.env.write_txn()?;
            peer.mempool.delete(&mut rwtxn, txid)?;
            rwtxn.commit()?;
            assert_eq!(source.rebroadcast_transaction(txid)?.peer_count, 1);
            let received = wait_for_transaction(&peer, txid).await?;
            assert_eq!(
                bincode::serialize(&received)?,
                bincode::serialize(&transaction)?
            );
            assert!(!source.net_task.task.is_finished());
            assert!(!peer.net_task.task.is_finished());
            Ok(())
        })
    }

    #[test]
    fn retry_deletes_invalid_transaction() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (temp_dir, node) = temp_node(&runtime).await?;
            let mut transaction = signed_transaction(&node, temp_dir.path())?;
            transaction.authorizations.clear();
            assert!(node.broadcast_transaction(&transaction).is_err());
            let mut rwtxn = node.env.write_txn()?;
            node.mempool.put(&mut rwtxn, &transaction)?;
            rwtxn.commit()?;
            super::relay_pending_transactions(
                &node.env,
                &node.mempool,
                &node.state,
                &node.net,
                None,
            )?;
            assert!(node.get_all_transactions()?.is_empty());
            Ok(())
        })
    }

    #[test]
    fn retry_recovers_from_a_closed_peer_queue() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (source_dir, source) = temp_node(&runtime).await?;
            let (closed_dir, closed) = temp_node(&runtime).await?;
            let (peer_dir, peer) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&source, source_dir.path())?;
            signed_transaction(&closed, closed_dir.path())?;
            signed_transaction(&peer, peer_dir.path())?;
            let txid = transaction.transaction.txid();
            source.submit_transaction(&transaction)?;
            let closed_addr = closed.net.server.local_addr()?;
            source.connect_peer(closed_addr)?;
            source.connect_peer(peer.net.server.local_addr()?)?;
            wait_for_transaction(&closed, txid).await?;
            wait_for_transaction(&peer, txid).await?;
            source.net_task.task.abort();
            while !source.net_task.task.is_finished() {
                tokio::task::yield_now().await;
            }
            source.net.try_with_active_peer_connection(closed_addr, |handle| {
                handle.internal_message_tx.close_channel();
            }).context("The peer connection is absent")?;
            let result = source.rebroadcast_transaction(txid);
            assert!(matches!(result, Err(crate::node::Error::Net(error))
                if matches!(*error, crate::net::Error::PushTransaction { .. })));
            let mut rwtxn = peer.env.write_txn()?;
            peer.mempool.delete(&mut rwtxn, txid)?;
            rwtxn.commit()?;
            super::relay_pending_transactions(&source.env, &source.mempool, &source.state, &source.net, None)?;
            let received = wait_for_transaction(&peer, txid).await?;
            assert_eq!(bincode::serialize(&received)?, bincode::serialize(&transaction)?);
            assert_eq!(source.get_all_transactions()?.len(), 1);
            Ok(())
        })
    }

    #[test]
    fn retry_returns_serialization_errors() {
        let error = crate::state::Error::BorshSerialize(std::io::Error::other(
            "The transaction serialization failed",
        ));
        assert!(super::transaction_read_error(&error));
        assert!(!super::transaction_read_error(
            &crate::state::Error::NotEnoughFees
        ));
    }

    #[test]
    fn peer_forward_recovers_from_a_closed_queue() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (source_dir, mut source) = temp_node(&runtime).await?;
            let (closed_dir, closed) = temp_node(&runtime).await?;
            let (peer_dir, peer) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&source, source_dir.path())?;
            signed_transaction(&closed, closed_dir.path())?;
            signed_transaction(&peer, peer_dir.path())?;
            source.net_task.task.abort();
            while !source.net_task.task.is_finished() {
                tokio::task::yield_now().await;
            }
            let (net, _native_info, _dial_seeds) = crate::net::Net::new(
                runtime.handle(),
                &source.env,
                source.archive.clone(),
                None,
                Network::Regtest,
                source.state.clone(),
                (Ipv4Addr::LOCALHOST, 0).into(),
                Default::default(),
                Default::default(),
            )?;
            source.net = net;
            let closed_addr = closed.net.server.local_addr()?;
            source.connect_peer(closed_addr)?;
            source.connect_peer(peer.net.server.local_addr()?)?;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if source
                        .get_active_peers()
                        .iter()
                        .filter(|peer| {
                            peer.status == PeerConnectionStatus::Connected
                        })
                        .count()
                        == 2
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            source
                .net
                .try_with_active_peer_connection(closed_addr, |handle| {
                    handle.internal_message_tx.close_channel();
                })
                .context("The peer connection is absent")?;
            let (peer_info_tx, peer_info_rx) =
                futures::channel::mpsc::unbounded();
            let task = test_net_task(&source, peer_info_rx);
            let task = tokio::spawn(task.run());
            peer_info_tx.unbounded_send((
                (Ipv4Addr::LOCALHOST, 1).into(),
                Some(crate::net::PeerConnectionInfo::NewTransaction(
                    transaction.clone(),
                )),
            ))?;
            let received =
                wait_for_transaction(&peer, transaction.transaction.txid())
                    .await?;
            assert_eq!(
                bincode::serialize(&received)?,
                bincode::serialize(&transaction)?
            );
            assert!(!task.is_finished());
            drop(peer_info_tx);
            let result =
                tokio::time::timeout(Duration::from_secs(5), task).await??;
            assert!(matches!(result, Err(super::Error::PeerInfoRxClosed)));
            Ok(())
        })
    }

    #[test]
    fn reconnect_replay_targets_the_new_peer() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (source_dir, source) = temp_node(&runtime).await?;
            let (first_dir, first) = temp_node(&runtime).await?;
            let (next_dir, next) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&source, source_dir.path())?;
            signed_transaction(&first, first_dir.path())?;
            signed_transaction(&next, next_dir.path())?;
            let txid = transaction.transaction.txid();
            source.submit_transaction(&transaction)?;
            source.connect_peer(first.net.server.local_addr()?)?;
            wait_for_transaction(&first, txid).await?;
            let mut rwtxn = first.env.write_txn()?;
            first.mempool.delete(&mut rwtxn, txid)?;
            rwtxn.commit()?;
            next.connect_peer(source.net.server.local_addr()?)?;
            wait_for_transaction(&next, txid).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(first.get_authorized_transaction(txid)?.is_none());
            Ok(())
        })
    }

    #[test]
    fn peer_without_inputs_stays_connected_for_retry() -> anyhow::Result<()> {
        use crate::net::{PeerConnectionInfo, PeerResponse};
        use futures::StreamExt as _;
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (source_dir, mut source) = temp_node(&runtime).await?;
            let (peer_dir, peer) = temp_node(&runtime).await?;
            let transaction = signed_transaction(&source, source_dir.path())?;
            let txid = transaction.transaction.txid();
            source.submit_transaction(&transaction)?;
            source.net_task.task.abort();
            while !source.net_task.task.is_finished() {
                tokio::task::yield_now().await;
            }
            let (net, mut info, _dial_seeds) = crate::net::Net::new(
                runtime.handle(), &source.env, source.archive.clone(), None,
                Network::Regtest, source.state.clone(),
                (Ipv4Addr::LOCALHOST, 0).into(), Default::default(), Default::default(),
            )?;
            source.net = net;
            let address = peer.net.server.local_addr()?;
            source.connect_peer(address)?;
            let (_, connected) = tokio::time::timeout(Duration::from_secs(5), info.next())
                .await?.context("The peer event stream closed")?;
            assert!(matches!(connected, Some(PeerConnectionInfo::Connected)));
            super::relay_pending_transactions(&source.env, &source.mempool, &source.state, &source.net, Some(address))?;
            let (_, response) = tokio::time::timeout(Duration::from_secs(5), info.next())
                .await?.context("The peer event stream closed")?;
            let Some(PeerConnectionInfo::Response(response)) = response else {
                anyhow::bail!("The peer returned no transaction response: {response:?}");
            };
            assert!(matches!(response.0, PeerResponse::TransactionRejected(rejected) if rejected == txid));
            assert!(peer.get_authorized_transaction(txid)?.is_none());
            signed_transaction(&peer, peer_dir.path())?;
            assert_eq!(source.rebroadcast_transaction(txid)?.peer_count, 1);
            let received = wait_for_transaction(&peer, txid).await?;
            assert_eq!(
                bincode::serialize(&received)?,
                bincode::serialize(&transaction)?
            );
            assert!(!peer.net_task.task.is_finished());
            Ok(())
        })
    }

    #[tokio::test(start_paused = true)]
    async fn retry_interval_limits_repeat_requests() -> anyhow::Result<()> {
        use futures::{FutureExt as _, StreamExt as _};
        let interval = super::transaction_retry_interval();
        futures::pin_mut!(interval);
        assert_eq!(interval.next().await, Some(()));
        tokio::time::advance(Duration::from_secs(59)).await;
        assert!(interval.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(interval.next().await, Some(()));
        tokio::time::advance(Duration::from_secs(180)).await;
        assert_eq!(interval.next().await, Some(()));
        assert!(interval.next().now_or_never().is_none());
        Ok(())
    }

    #[test]
    fn peer_transaction_conflicts_do_not_stop_net_task() -> anyhow::Result<()> {
        use std::collections::HashMap;

        use futures::channel::mpsc;
        use heed::types::SerdeBincode;
        use sneed::DatabaseUnique;

        use super::{Error, NetTask, NetTaskContext};
        use crate::{
            net::PeerConnectionInfo,
            types::{FilledOutput, OutPoint, OutPointKey, Transaction},
            wallet::Wallet,
        };

        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (temp_dir, node) = temp_node(&runtime).await?;
            node.net_task.task.abort();
            while !node.net_task.task.is_finished() {
                tokio::task::yield_now().await;
            }
            let wallet = Wallet::new(&temp_dir.path().join("wallet"))?;
            wallet.set_seed(&[2; 64])?;
            let address = wallet.get_new_address()?;
            let mut inputs = Vec::new();
            let mut utxos = HashMap::new();
            let mut rwtxn = node.env.write_txn()?;
            let state_utxos = DatabaseUnique::<
                OutPointKey,
                SerdeBincode<FilledOutput>,
            >::create(
                &node.env, &mut rwtxn, "utxos"
            )?;
            for index in 0..2 {
                let outpoint = OutPoint::Regular {
                    txid: [index; 32].into(),
                    vout: 0,
                };
                let output = FilledOutput::new_bitcoin_value(
                    address,
                    bitcoin::Amount::from_sat(1_000),
                );
                state_utxos.put(
                    &mut rwtxn,
                    &OutPointKey::from(&outpoint),
                    &output,
                )?;
                inputs.push(outpoint);
                utxos.insert(outpoint, output);
            }
            wallet.put_utxos(&utxos)?;
            let make_tx = |inputs, value| -> anyhow::Result<_> {
                let tx = Transaction::new(
                    inputs,
                    vec![
                        FilledOutput::new_bitcoin_value(
                            address,
                            bitcoin::Amount::from_sat(value),
                        )
                        .into(),
                    ],
                );
                let tx = wallet.authorize(tx)?;
                node.state.validate_transaction(&rwtxn, &tx)?;
                Ok(tx)
            };
            let first_tx = make_tx(vec![inputs[0]], 900)?;
            let conflict_tx = make_tx(vec![inputs[1], inputs[0]], 1_800)?;
            let next_tx = make_tx(vec![inputs[1]], 900)?;
            node.mempool.put(&mut rwtxn, &first_tx)?;
            rwtxn.commit()?;

            let (
                forward_mainchain_task_request_tx,
                forward_mainchain_task_request_rx,
            ) = mpsc::unbounded();
            let (_mainchain_task_event_tx, mainchain_task_event_rx) =
                mpsc::unbounded();
            let (new_tip_ready_tx, new_tip_ready_rx) = mpsc::unbounded();
            let (peer_info_tx, peer_info_rx) = mpsc::unbounded();
            let task = NetTask {
                ctxt: NetTaskContext {
                    env: node.env.clone(),
                    archive: node.archive.clone(),
                    mainchain_task: node.mainchain_task.clone(),
                    mempool: node.mempool.clone(),
                    net: node.net.clone(),
                    state: node.state.clone(),
                    #[cfg(feature = "zmq")]
                    zmq_pub_handler: node.zmq_pub_handler.clone(),
                },
                forward_mainchain_task_request_tx,
                forward_mainchain_task_request_rx,
                mainchain_task_event_rx,
                new_tip_ready_tx,
                new_tip_ready_rx,
                peer_info_rx,
            };
            for tx in [&first_tx, &conflict_tx, &next_tx] {
                peer_info_tx.unbounded_send((
                    (Ipv4Addr::LOCALHOST, 1).into(),
                    Some(PeerConnectionInfo::NewTransaction(tx.clone())),
                ))?;
            }
            drop(peer_info_tx);
            let result =
                tokio::time::timeout(Duration::from_secs(5), task.run())
                    .await?;
            assert!(
                matches!(result, Err(Error::PeerInfoRxClosed)),
                "the network task stopped before the mailbox closed: {result:?}"
            );
            let rotxn = node.env.read_txn()?;
            for tx in [&first_tx, &next_tx] {
                assert!(
                    node.mempool
                        .transactions
                        .try_get(&rotxn, &tx.transaction.txid())?
                        .is_some()
                );
            }
            assert!(
                node.mempool
                    .transactions
                    .try_get(&rotxn, &conflict_tx.transaction.txid())?
                    .is_none()
            );
            Ok(())
        })
    }

    #[test]
    fn retry_connection_timeout_before_first_message() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (_temp_dir, node) = temp_node(&runtime).await?;
            let silent_peer =
                tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let addr = silent_peer.local_addr()?;
            node.connect_peer(addr)?;
            assert_eq!(node.get_active_peers().len(), 1);
            assert_eq!(
                node.net.try_with_active_peer_connection(addr, |peer| peer
                    .received_msg_successfully(),),
                Some(false)
            );

            tokio::time::timeout(Duration::from_secs(35), async {
                while !node.get_active_peers().is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("the QUIC connection did not time out")?;
            drop(silent_peer);
            let (remote, _) =
                make_server_endpoint(addr, std::collections::HashSet::new())?;
            let retry = tokio::time::timeout(Duration::from_secs(15), async {
                remote
                    .accept()
                    .await
                    .context("the endpoint closed before the retry")?
                    .await
                    .map_err(anyhow::Error::from)
            })
            .await
            .context("the node did not retry the connection timeout")??;
            assert!(retry.close_reason().is_none());
            remote.close(0_u32.into(), b"test complete");
            Ok(())
        })
    }

    #[test]
    fn retry_duplicate_close_before_first_message() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let (_temp_dir, node) = temp_node(&runtime).await?;
            let (remote, _) = make_server_endpoint(
                (Ipv4Addr::LOCALHOST, 0).into(),
                std::collections::HashSet::new(),
            )?;
            let addr = remote.local_addr()?;
            node.connect_peer(addr)?;
            let first =
                tokio::time::timeout(Duration::from_secs(5), remote.accept())
                    .await?
                    .context("the first connection did not arrive")?
                    .await?;
            assert_eq!(
                node.net.try_with_active_peer_connection(addr, |peer| peer
                    .received_msg_successfully(),),
                Some(false)
            );

            let mut connection = first;
            for _ in 0..2 {
                let closed_at = tokio::time::Instant::now();
                connection.close(1_u32.into(), b"already connected");
                let retry = tokio::time::timeout(
                    Duration::from_secs(15),
                    remote.accept(),
                )
                .await
                .context("the node did not retry the duplicate close")?
                .context("the endpoint closed before the retry")?
                .await?;
                assert!(closed_at.elapsed() >= Duration::from_secs(10));
                assert_eq!(retry.remote_address(), connection.remote_address());
                connection = retry;
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if node.get_active_peers().iter().any(|peer| {
                        peer.address == addr
                            && peer.status == PeerConnectionStatus::Connected
                    }) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            remote.close(0_u32.into(), b"test complete");
            Ok(())
        })
    }
}
