//! RPC API

use plain_bitassets::types::{Block, BlockHash, OutPoint, SpentOutput, TxIn};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub use plain_bitassets::node::BroadcastResult;

mod schema;
#[cfg(test)]
mod test;

fn build_openapi(
    build: fn() -> utoipa::openapi::OpenApi,
) -> std::io::Result<utoipa::openapi::OpenApi> {
    // The generated `RpcDoc::openapi` builds every schema in one stack frame.
    // That frame does not fit a default 2MB thread stack.
    const STACK_SIZE: usize = 16 * 1024 * 1024;
    std::thread::Builder::new()
        .name("openapi-builder".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(build)?
        .join()
        .map_err(|_panic| {
            std::io::Error::other("the OpenAPI builder thread panicked")
        })
}

/// Build the OpenAPI document that describes all RPC methods.
///
/// # Errors
/// Fails when the builder thread does not start, or panics.
pub fn openapi() -> std::io::Result<utoipa::openapi::OpenApi> {
    build_openapi(|| {
        use utoipa::OpenApi as _;
        let mut res = open_api::RpcDoc::openapi();
        res.merge(node::PrivateRpcDoc::openapi());
        res.merge(node::RpcDoc::openapi());
        res.merge(wallet::RpcDoc::openapi());
        res
    })
}

/// Build the OpenAPI document that describes the public RPC methods.
///
/// # Errors
/// Fails when the builder thread does not start, or panics.
pub fn public_openapi() -> std::io::Result<utoipa::openapi::OpenApi> {
    build_openapi(|| {
        use utoipa::OpenApi as _;
        let mut res = open_api::RpcDoc::openapi();
        res.merge(node::RpcDoc::openapi());
        res
    })
}

/// Build the OpenAPI document that describes the private RPC methods.
///
/// # Errors
/// Fails when the builder thread does not start, or panics.
pub fn private_openapi() -> std::io::Result<utoipa::openapi::OpenApi> {
    build_openapi(|| {
        use utoipa::OpenApi as _;
        let mut res = open_api::RpcDoc::openapi();
        res.merge(node::PrivateRpcDoc::openapi());
        res.merge(wallet::RpcDoc::openapi());
        res
    })
}

/// A spent output, and the outpoint that created it
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct PointedSpentOutput {
    pub outpoint: OutPoint,
    pub output: SpentOutput,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TxInfo {
    pub confirmations: Option<u32>,
    pub fee_sats: u64,
    pub txin: Option<TxIn>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct GetBlockTemplateResponse {
    /// Block hash to commit to in a BMM request
    pub critical_hash: BlockHash,
    /// Block to pass to `connect_block` once its BMM request is included in a
    /// mainchain block
    pub block: Block,
    /// Fees collected by the transactions in the block, in sats
    pub fees_sats: u64,
}

pub mod open_api {
    use jsonrpsee::{core::RpcResult, proc_macros::rpc};
    use l2l_openapi::open_api;

    use crate::schema;

    #[open_api]
    #[rpc(client, server)]
    pub trait Rpc {
        /// Get OpenRPC schema
        #[open_api_method(output_schema(ToSchema = "schema::OpenApi"))]
        #[method(name = "openapi_schema")]
        async fn openapi_schema(&self) -> RpcResult<utoipa::openapi::OpenApi>;
    }
}

pub mod node {
    use std::{collections::HashSet, net::SocketAddr};

    use fraction::Fraction;
    use jsonrpsee::{core::RpcResult, proc_macros::rpc};
    use l2l_openapi::open_api;
    use plain_bitassets::{
        authorization::Signature,
        net::{Peer, PeerConnectionStatus},
        state::{AmmPoolState, BitAssetSeqId, DutchAuctionState},
        types::{
            Address, AssetId, Authorization, Authorized, BitAssetData,
            BitAssetDataUpdates, BitAssetId, BitcoinOutputContent, Block,
            BlockHash, BlockIndex, BlockIndexDeposit, BlockIndexSpend,
            BlockIndexTx, Body, Coinbase, DutchAuctionId, DutchAuctionParams,
            EncryptionPubKey, FilledOutput, FilledOutputContent, Header,
            InPoint, Inputs, M6id, MainchainSyncPhase, MainchainSyncProgress,
            MempoolTx, MerkleRoot, OutPoint, Output, OutputContent, Outputs,
            PointedOutput, SpentOutput, Transaction, TxData, TxIn, Txid,
            VerifyingKey, WithdrawalBundle, WithdrawalOutputContent,
            schema as bitassets_schema,
        },
    };

    use crate::{
        BroadcastResult, PointedSpentOutput, TxInfo, open_api, schema,
    };

    #[open_api(ref_schemas[
        bitassets_schema::BitcoinAddr, bitassets_schema::BitcoinBlockHash,
        bitassets_schema::BitcoinTransaction, bitassets_schema::BitcoinOutPoint,
        bitassets_schema::SocketAddr, Address, AssetId, Authorization,
        BitAssetData, BitAssetDataUpdates, BitAssetId, BitcoinOutputContent,
        Block, BlockHash, BlockIndexDeposit, BlockIndexSpend, BlockIndexTx,
        Body, Coinbase, DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
        FilledOutput, FilledOutputContent, Header, InPoint, M6id,
        Inputs, MainchainSyncPhase, MerkleRoot, OutPoint, Output, OutputContent,
        Outputs,
        PeerConnectionStatus, Signature, SpentOutput, Transaction, TxData,
        Txid, TxIn, WithdrawalOutputContent, VerifyingKey,
    ])]
    #[rpc(client, server, server_bounds(Self: open_api::RpcServer))]
    pub trait PrivateRpc {
        /// Connect to a peer
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "connect_peer")]
        async fn connect_peer(
            &self,
            #[open_api_method_arg(schema(
                ToSchema = "bitassets_schema::SocketAddr"
            ))]
            addr: SocketAddr,
        ) -> RpcResult<()>;

        /// Delete peer from known_peers DB.
        /// Connections to the peer are not terminated.
        #[method(name = "forget_peer")]
        async fn forget_peer(
            &self,
            #[open_api_method_arg(schema(
                PartialSchema = "bitassets_schema::SocketAddr"
            ))]
            addr: SocketAddr,
        ) -> RpcResult<()>;

        /// Invalidate a block and its descendants. If the tip descends from the
        /// block, re-org to the parent of the block.
        #[method(name = "invalidate_block")]
        async fn invalidate_block(
            &self,
            block_hash: BlockHash,
        ) -> RpcResult<()>;

        /// Remove a tx from the mempool
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "remove_from_mempool")]
        async fn remove_from_mempool(&self, txid: Txid) -> RpcResult<()>;

        /// Stop the node
        #[method(name = "stop")]
        async fn stop(&self);
    }

    #[open_api(ref_schemas[
        bitassets_schema::BitcoinAddr, bitassets_schema::BitcoinBlockHash,
        bitassets_schema::BitcoinTransaction, bitassets_schema::BitcoinOutPoint,
        bitassets_schema::SocketAddr, Address, AssetId, Authorization,
        BitAssetData, BitAssetDataUpdates, BitAssetId, BitcoinOutputContent,
        Block, BlockHash, BlockIndexDeposit, BlockIndexSpend, BlockIndexTx,
        Body, Coinbase, DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
        FilledOutput, FilledOutputContent, Header, InPoint, M6id,
        Inputs, MainchainSyncPhase, MerkleRoot, OutPoint, Output, OutputContent,
        Outputs,
        PeerConnectionStatus, Signature, SpentOutput, Transaction, TxData,
        Txid, TxIn, WithdrawalOutputContent, VerifyingKey,
    ])]
    #[rpc(client, server, server_bounds(Self: open_api::RpcServer))]
    pub trait Rpc {
        /// Retrieve data for a single BitAsset
        #[method(name = "bitasset_data")]
        async fn bitasset_data(
            &self,
            bitasset_id: BitAssetId,
        ) -> RpcResult<BitAssetData>;

        /// List all BitAssets
        #[open_api_method(output_schema(PartialSchema = "schema::Array<
                schema::ArrayTuple3<BitAssetSeqId, BitAssetId, BitAssetData>
            >"))]
        #[method(name = "bitassets")]
        async fn bitassets(
            &self,
        ) -> RpcResult<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>>;

        /// Connect a block for which a BMM request was included in the specified
        /// mainchain block. Returns `true` if it was accepted as the new tip.
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "connect_block")]
        async fn connect_block(
            &self,
            block: Block,
            #[open_api_method_arg(schema(
                PartialSchema = "bitassets_schema::BitcoinBlockHash"
            ))]
            main_block_hash: bitcoin::BlockHash,
        ) -> RpcResult<bool>;

        /// List all Dutch auctions
        #[open_api_method(output_schema(
            PartialSchema = "schema::Array<schema::ArrayTuple<DutchAuctionId, serde_json::Value>>"
        ))]
        #[method(name = "dutch_auctions")]
        async fn dutch_auctions(
            &self,
        ) -> RpcResult<Vec<(DutchAuctionId, DutchAuctionState)>>;

        /// Get the state of the specified AMM pool
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "get_amm_pool_state")]
        async fn get_amm_pool_state(
            &self,
            asset0: AssetId,
            asset1: AssetId,
        ) -> RpcResult<AmmPoolState>;

        /// Get the current price for the specified pair
        #[open_api_method(output_schema(
            PartialSchema = "schema::Optional<schema::Fraction>"
        ))]
        #[method(name = "get_amm_price")]
        async fn get_amm_price(
            &self,
            base: AssetId,
            quote: AssetId,
        ) -> RpcResult<Option<Fraction>>;

        /// Get block data
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "get_block")]
        async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block>;

        /// Get the block hash at the specified height in the current chain,
        /// if it exists
        #[open_api_method(output_schema(
            PartialSchema = "schema::Optional<BlockHash>"
        ))]
        #[method(name = "get_block_hash")]
        async fn get_block_hash(
            &self,
            height: u32,
        ) -> RpcResult<Option<BlockHash>>;

        /// Get the transaction ids, sizes and encodings of a block, with the
        /// mainchain deposits and withdrawal bundle spends it applied
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "get_block_index")]
        async fn get_block_index(
            &self,
            block_hash: BlockHash,
        ) -> RpcResult<BlockIndex>;

        /// Get mainchain blocks that commit to a specified block hash
        #[open_api_method(output_schema(
            PartialSchema = "bitassets_schema::BitcoinBlockHash"
        ))]
        #[method(name = "get_bmm_inclusions")]
        async fn get_bmm_inclusions(
            &self,
            block_hash: plain_bitassets::types::BlockHash,
        ) -> RpcResult<Vec<bitcoin::BlockHash>>;

        /// Get the best mainchain block hash known by Thunder
        #[open_api_method(output_schema(
            PartialSchema = "schema::Optional<bitassets_schema::BitcoinBlockHash>"
        ))]
        #[method(name = "get_best_mainchain_block_hash")]
        async fn get_best_mainchain_block_hash(
            &self,
        ) -> RpcResult<Option<bitcoin::BlockHash>>;

        /// Get the best sidechain block hash known by BitAssets
        #[open_api_method(output_schema(
            PartialSchema = "schema::Optional<BlockHash>"
        ))]
        #[method(name = "get_best_sidechain_block_hash")]
        async fn get_best_sidechain_block_hash(
            &self,
        ) -> RpcResult<Option<BlockHash>>;

        /// Get stxos for addresses
        #[method(name = "get_stxos")]
        async fn get_stxos(
            &self,
            addresses: HashSet<Address>,
        ) -> RpcResult<Vec<PointedSpentOutput>>;

        /// Get transaction by txid
        #[method(name = "get_transaction")]
        async fn get_transaction(
            &self,
            txid: Txid,
        ) -> RpcResult<Option<Transaction>>;

        /// Get information about a transaction in the current chain
        #[method(name = "get_transaction_info")]
        async fn get_transaction_info(
            &self,
            txid: Txid,
        ) -> RpcResult<Option<TxInfo>>;

        /// Get utxos for addresses
        #[method(name = "get_utxos")]
        async fn get_utxos(
            &self,
            addresses: HashSet<Address>,
        ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

        /// Get the current block count
        #[method(name = "getblockcount")]
        async fn getblockcount(&self) -> RpcResult<u32>;

        /// Get the height of the latest failed withdrawal bundle
        #[method(name = "latest_failed_withdrawal_bundle_height")]
        async fn latest_failed_withdrawal_bundle_height(
            &self,
        ) -> RpcResult<Option<u32>>;

        /// List the transactions the mempool holds, in no particular order.
        #[method(name = "list_mempool")]
        async fn list_mempool(&self) -> RpcResult<Vec<MempoolTx>>;

        /// List peers
        #[method(name = "list_peers")]
        async fn list_peers(&self) -> RpcResult<Vec<Peer>>;

        /// List all UTXOs
        #[open_api_method(output_schema(
            ToSchema = "Vec<PointedOutput<FilledOutputContent>>"
        ))]
        #[method(name = "list_utxos")]
        async fn list_utxos(
            &self,
        ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

        /// Get the progress of the startup sync with the mainchain
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "mainchain_sync_progress")]
        async fn mainchain_sync_progress(
            &self,
        ) -> RpcResult<MainchainSyncProgress>;

        /// Get pending withdrawal bundle
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "pending_withdrawal_bundle")]
        async fn pending_withdrawal_bundle(
            &self,
        ) -> RpcResult<Option<WithdrawalBundle>>;

        /// Get total sidechain wealth in sats
        #[method(name = "sidechain_wealth")]
        async fn sidechain_wealth_sats(&self) -> RpcResult<u64>;

        /// Get a signed transaction from the mempool.
        #[open_api_method(output_schema(
            ToSchema = "Option<Authorized<Transaction>>"
        ))]
        #[method(name = "get_authorized_transaction")]
        async fn get_authorized_transaction(
            &self,
            txid: Txid,
        ) -> RpcResult<Option<Authorized<Transaction>>>;

        /// Validate a signed transaction and send it to connected peer queues.
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "broadcast_transaction")]
        async fn broadcast_transaction(
            &self,
            transaction: Authorized<Transaction>,
        ) -> RpcResult<BroadcastResult>;

        /// Send a signed mempool transaction to connected peer queues.
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "rebroadcast_transaction")]
        async fn rebroadcast_transaction(
            &self,
            txid: Txid,
        ) -> RpcResult<BroadcastResult>;

        /// Validate and broadcast a transaction.
        #[method(name = "submit_transaction")]
        async fn submit_transaction(
            &self,
            transaction: Authorized<Transaction>,
        ) -> RpcResult<Txid>;
    }
}

pub mod wallet {
    use jsonrpsee::{core::RpcResult, proc_macros::rpc};
    use l2l_openapi::open_api;
    use plain_bitassets::{
        authorization::{Dst, Signature},
        net::PeerConnectionStatus,
        types::{
            Address, AssetId, Authorization, Authorized, BitAssetData,
            BitAssetDataUpdates, BitAssetId, BitcoinOutputContent, Block,
            BlockHash, BlockIndexDeposit, BlockIndexSpend, BlockIndexTx, Body,
            Coinbase, DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
            FilledOutput, FilledOutputContent, Header, InPoint, Inputs, M6id,
            MainchainSyncPhase, MerkleRoot, OutPoint, Output, OutputContent,
            Outputs, PointedOutput, SpentOutput, Transaction, TxData, TxIn,
            Txid, VerifyingKey, WithdrawalOutputContent,
            schema as bitassets_schema,
        },
        wallet::{Balance, TransferDests},
    };

    use crate::{GetBlockTemplateResponse, open_api, schema};

    #[open_api(ref_schemas[
        bitassets_schema::BitcoinAddr, bitassets_schema::BitcoinBlockHash,
        bitassets_schema::BitcoinTransaction, bitassets_schema::BitcoinOutPoint,
        bitassets_schema::SocketAddr, Address, AssetId, Authorization,
        BitAssetData, BitAssetDataUpdates, BitAssetId, BitcoinOutputContent,
        Block, BlockHash, BlockIndexDeposit, BlockIndexSpend, BlockIndexTx,
        Body, Coinbase, DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
        FilledOutput, FilledOutputContent, Header, InPoint, M6id,
        Inputs, MainchainSyncPhase, MerkleRoot, OutPoint, Output, OutputContent,
        Outputs,
        PeerConnectionStatus, Signature, SpentOutput, Transaction, TxData,
        Txid, TxIn, WithdrawalOutputContent, VerifyingKey,
    ])]
    #[rpc(client, server, server_bounds(Self: open_api::RpcServer))]
    pub trait Rpc {
        /// Burn an AMM position
        #[method(name = "amm_burn")]
        async fn amm_burn(
            &self,
            asset0: AssetId,
            asset1: AssetId,
            lp_token_amount: u64,
        ) -> RpcResult<Txid>;

        /// Mint an AMM position
        #[method(name = "amm_mint")]
        async fn amm_mint(
            &self,
            asset0: AssetId,
            asset1: AssetId,
            amount0: u64,
            amount1: u64,
        ) -> RpcResult<Txid>;

        /// Returns the amount of `asset_receive` to receive
        #[method(name = "amm_swap")]
        async fn amm_swap(
            &self,
            asset_spend: AssetId,
            asset_receive: AssetId,
            amount_spend: u64,
        ) -> RpcResult<u64>;

        /// Balance in sats
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "bitcoin_balance")]
        async fn bitcoin_balance(&self) -> RpcResult<Balance>;

        /// Deposit to address
        #[open_api_method(output_schema(
            PartialSchema = "schema::BitcoinTxid"
        ))]
        #[method(name = "create_deposit")]
        async fn create_deposit(
            &self,
            address: Address,
            value_sats: u64,
            fee_sats: u64,
        ) -> RpcResult<bitcoin::Txid>;

        /// Decrypt a message with the specified encryption key corresponding to
        /// the specified encryption pubkey.
        /// Returns a decrypted hex string.
        #[method(name = "decrypt_msg")]
        async fn decrypt_msg(
            &self,
            encryption_pubkey: EncryptionPubKey,
            ciphertext: String,
        ) -> RpcResult<String>;

        /// Returns the amount of the base asset to receive
        #[method(name = "dutch_auction_bid")]
        async fn dutch_auction_bid(
            &self,
            dutch_auction_id: DutchAuctionId,
            bid_size: u64,
        ) -> RpcResult<u64>;

        /// Create a dutch auction
        #[method(name = "dutch_auction_create")]
        async fn dutch_auction_create(
            &self,
            #[open_api_method_arg(schema(ToSchema))]
            dutch_auction_params: DutchAuctionParams,
        ) -> RpcResult<Txid>;

        /// Returns the amount of the base asset and quote asset to receive
        #[open_api_method(output_schema(
            PartialSchema = "schema::ArrayTuple<u64, u64>"
        ))]
        #[method(name = "dutch_auction_collect")]
        async fn dutch_auction_collect(
            &self,
            dutch_auction_id: DutchAuctionId,
        ) -> RpcResult<(u64, u64)>;

        /// Encrypt a message to the specified encryption pubkey
        /// Returns the ciphertext as a hex string.
        #[method(name = "encrypt_msg")]
        async fn encrypt_msg(
            &self,
            encryption_pubkey: EncryptionPubKey,
            msg: String,
        ) -> RpcResult<String>;

        /// Format a deposit address
        #[method(name = "format_deposit_address")]
        async fn format_deposit_address(
            &self,
            address: Address,
        ) -> RpcResult<String>;

        /// Generate a mnemonic seed phrase
        #[method(name = "generate_mnemonic")]
        async fn generate_mnemonic(&self) -> RpcResult<String>;

        /// Assemble a block to blind merge mine, without requesting BMM for it.
        /// The caller requests BMM for `critical_hash` itself, then passes the
        /// block back to `connect_block`.
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "get_block_template")]
        async fn get_block_template(
            &self,
        ) -> RpcResult<GetBlockTemplateResponse>;

        /// Get a new address
        #[method(name = "get_new_address")]
        async fn get_new_address(&self) -> RpcResult<Address>;

        /// Get new encryption key
        #[method(name = "get_new_encryption_key")]
        async fn get_new_encryption_key(&self) -> RpcResult<EncryptionPubKey>;

        /// Get new verifying/signing key
        #[method(name = "get_new_verifying_key")]
        async fn get_new_verifying_key(&self) -> RpcResult<VerifyingKey>;

        /// Get wallet addresses, sorted by base58 encoding
        #[method(name = "get_wallet_addresses")]
        async fn get_wallet_addresses(&self) -> RpcResult<Vec<Address>>;

        /// Get wallet UTXOs
        #[method(name = "get_wallet_utxos")]
        async fn get_wallet_utxos(
            &self,
        ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

        /// Attempt to mine a sidechain block
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "mine")]
        async fn mine(&self, fee: Option<u64>) -> RpcResult<()>;

        /*
        #[method(name = "my_unconfirmed_stxos")]
        async fn my_unconfirmed_stxos(&self) -> RpcResult<Vec<InPoint>>;
        */

        /// List unconfirmed owned UTXOs
        #[method(name = "my_unconfirmed_utxos")]
        async fn my_unconfirmed_utxos(&self) -> RpcResult<Vec<PointedOutput>>;

        /// List owned UTXOs
        #[method(name = "my_utxos")]
        async fn my_utxos(
            &self,
        ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

        /// Register a BitAsset
        #[method(name = "register_bitasset")]
        async fn register_bitasset(
            &self,
            plain_name: String,
            initial_supply: u64,
            bitasset_data: Option<BitAssetData>,
        ) -> RpcResult<Txid>;

        /// Reserve a BitAsset
        #[method(name = "reserve_bitasset")]
        async fn reserve_bitasset(&self, plain_name: String)
        -> RpcResult<Txid>;

        /// Set the wallet seed from a mnemonic seed phrase
        #[open_api_method(output_schema(ToSchema))]
        #[method(name = "set_seed_from_mnemonic")]
        async fn set_seed_from_mnemonic(
            &self,
            mnemonic: String,
        ) -> RpcResult<()>;

        /// Sign an arbitrary message with the specified verifying key
        #[method(name = "sign_arbitrary_msg")]
        async fn sign_arbitrary_msg(
            &self,
            verifying_key: VerifyingKey,
            msg: String,
        ) -> RpcResult<Signature>;

        /// Sign an arbitrary message with the secret key for the specified address
        #[method(name = "sign_arbitrary_msg_as_addr")]
        async fn sign_arbitrary_msg_as_addr(
            &self,
            address: Address,
            msg: String,
        ) -> RpcResult<Authorization>;

        /// Sign a transaction, and optionally broadcast it.
        #[method(name = "sign_transaction")]
        async fn sign_transaction(
            &self,
            transaction: Transaction,
            broadcast: Option<bool>,
        ) -> RpcResult<Authorized<Transaction>>;

        /// Transfer funds to the specified address
        #[method(name = "transfer")]
        async fn transfer(
            &self,
            dest: Address,
            value: u64,
            fee: u64,
            memo: Option<String>,
        ) -> RpcResult<Txid>;

        /// Transfer funds to each address in `dests`, which maps an address to a
        /// value in sats
        #[method(name = "transfer_many")]
        async fn transfer_many(
            &self,
            dests: TransferDests,
            fee_sats: u64,
        ) -> RpcResult<Txid>;

        /// Transfer bitassets to the specified address
        #[method(name = "transfer_bitasset")]
        async fn transfer_bitasset(
            &self,
            dest: Address,
            asset_id: BitAssetId,
            amount: u64,
            fee_sats: u64,
            memo: Option<String>,
        ) -> RpcResult<Txid>;

        /// Verify a signature on a message against the specified verifying key.
        /// Returns `true` if the signature is valid
        #[method(name = "verify_signature")]
        async fn verify_signature(
            &self,
            signature: Signature,
            verifying_key: VerifyingKey,
            dst: Dst,
            msg: String,
        ) -> RpcResult<bool>;

        /// Initiate a withdrawal to the specified mainchain address
        #[method(name = "withdraw")]
        async fn withdraw(
            &self,
            #[open_api_method_arg(schema(
                PartialSchema = "bitassets_schema::BitcoinAddr"
            ))]
            mainchain_address: bitcoin::Address<
                bitcoin::address::NetworkUnchecked,
            >,
            amount_sats: u64,
            fee_sats: u64,
            mainchain_fee_sats: u64,
        ) -> RpcResult<Txid>;
    }
}
