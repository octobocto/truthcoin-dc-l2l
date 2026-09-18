use std::cmp::Ordering;

use borsh::BorshSerialize;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::output::{Output, borsh_serialize_bitcoin_amount};
use crate::types::{
    GetBitcoinValue,
    hashes::{self, Hash, OutputsMerkleRoot},
};

pub mod error {
    use thiserror::Error;

    #[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
    pub enum MergeCbmtNodes {
        #[error("canonical size overflow")]
        SizeOverflow,
        #[error("bitcoin value overflow")]
        ValueOverflow,
    }

    #[derive(Debug, Error)]
    pub enum ComputeMerkleRoot {
        #[error("failed to merge CBMT nodes")]
        MergeCbmtNodes(#[from] MergeCbmtNodes),
        #[error("failed to compute canonical size for output ({index})")]
        CanonicalSize {
            index: usize,
            source: borsh::io::Error,
        },
    }
}

/// Hash to get a [`CbmtNode`] inner commitment for a leaf value
#[derive(BorshSerialize, Debug)]
struct CbmtLeafPreCommitment<'a> {
    #[borsh(serialize_with = "borsh_serialize_bitcoin_amount")]
    value: bitcoin::Amount,
    canonical_size: u64,
    output: &'a Output,
}

/// Hash to get a [`CbmtNode`] inner commitment for a non-leaf value
#[derive(BorshSerialize, Debug)]
struct CbmtNodePreCommitment {
    /// left child inner commitment
    left_commitment: Hash,
    /// Sum of child values
    #[borsh(serialize_with = "borsh_serialize_bitcoin_amount")]
    value: bitcoin::Amount,
    /// Sum of canonical sizes of children
    canonical_size: u64,
    /// right child inner commitment
    right_commitment: Hash,
}

/// Internal node of a CBMT
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CbmtNode {
    /// Commitment to child nodes or leaf value
    commitment: Hash,
    /// Sum of values for child nodes or leaf value
    value: bitcoin::Amount,
    /// Sum of canonical sizes for child nodes or leaf value
    canonical_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CbmtNodeResult {
    value: Result<CbmtNode, error::MergeCbmtNodes>,
    /// CBT index. `CbmtNode` orders by this, and nothing else.
    index: usize,
}

impl Default for CbmtNodeResult {
    fn default() -> Self {
        Self {
            value: Ok(CbmtNode::default()),
            index: 0,
        }
    }
}

impl PartialOrd for CbmtNodeResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CbmtNodeResult {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index.cmp(&other.index)
    }
}

/// Marker type for merging branch commitments with branch value totals and
/// branch canonical size totals.
struct MergeValueSizeTotal;

impl merkle_cbt::merkle_tree::Merge for MergeValueSizeTotal {
    type Item = CbmtNodeResult;

    fn merge(lnode: &Self::Item, rnode: &Self::Item) -> Self::Item {
        assert_eq!(lnode.index + 1, rnode.index);
        let index = (lnode.index - 1) / 2;
        let lnode = match lnode.value.as_ref() {
            Ok(lnode) => lnode,
            Err(err) => {
                return CbmtNodeResult {
                    value: Err(*err),
                    index,
                };
            }
        };
        let rnode = match rnode.value.as_ref() {
            Ok(rnode) => rnode,
            Err(err) => {
                return CbmtNodeResult {
                    value: Err(*err),
                    index,
                };
            }
        };
        let Some(value) = lnode.value.checked_add(rnode.value) else {
            return CbmtNodeResult {
                value: Err(error::MergeCbmtNodes::ValueOverflow),
                index,
            };
        };
        let Some(canonical_size) =
            lnode.canonical_size.checked_add(rnode.canonical_size)
        else {
            return CbmtNodeResult {
                value: Err(error::MergeCbmtNodes::SizeOverflow),
                index,
            };
        };
        let commitment = hashes::hash(&CbmtNodePreCommitment {
            left_commitment: lnode.commitment,
            value,
            canonical_size,
            right_commitment: rnode.commitment,
        });
        CbmtNodeResult {
            value: Ok(CbmtNode {
                commitment,
                value,
                canonical_size,
            }),
            index,
        }
    }
}

/// Complete binary merkle tree with annotated value and canonical size totals
type CbmtWithValueSizeTotal =
    merkle_cbt::CBMT<CbmtNodeResult, MergeValueSizeTotal>;

#[derive(
    BorshSerialize, Clone, Debug, Default, Deserialize, Serialize, ToSchema,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Outputs(pub Vec<Output>);

impl Outputs {
    #[inline(always)]
    pub fn as_slice(&self) -> &[Output] {
        self.0.as_slice()
    }

    fn merkle_leaves(
        &self,
    ) -> Result<Vec<CbmtNodeResult>, error::ComputeMerkleRoot> {
        let n_outputs = self.len();
        self.iter()
            .enumerate()
            .map(|(index, output)| -> Result<_, error::ComputeMerkleRoot> {
                let value = output.get_bitcoin_value();
                let canonical_size =
                    output.canonical_size().map_err(|err| {
                        error::ComputeMerkleRoot::CanonicalSize {
                            index,
                            source: err,
                        }
                    })?;
                let leaf_pre_commitment = CbmtLeafPreCommitment {
                    value,
                    canonical_size,
                    output,
                };
                Ok(CbmtNodeResult {
                    value: Ok(CbmtNode {
                        commitment: hashes::hash(&leaf_pre_commitment),
                        value,
                        canonical_size,
                    }),
                    index: (index + n_outputs) - 1,
                })
            })
            .collect::<Result<_, _>>()
    }

    pub(crate) fn compute_merkle_root(
        &self,
    ) -> Result<OutputsMerkleRoot, error::ComputeMerkleRoot> {
        let CbmtNode { commitment, .. } =
            CbmtWithValueSizeTotal::build_merkle_root(
                self.merkle_leaves()?.as_slice(),
            )
            .value?;
        Ok(commitment.into())
    }

    #[inline(always)]
    pub fn extend<I>(&mut self, outputs: I)
    where
        I: IntoIterator<Item = Output>,
    {
        self.0.extend(outputs)
    }

    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&Output> {
        self.0.get(index)
    }

    #[inline(always)]
    pub fn last(&self) -> Option<&Output> {
        self.0.last()
    }

    #[inline(always)]
    pub fn rotate_right(&mut self, mid: usize) {
        self.0.rotate_right(mid)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline(always)]
    pub fn iter(&self) -> std::slice::Iter<'_, Output> {
        self.0.iter()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn push(&mut self, output: Output) {
        self.0.push(output)
    }

    #[inline(always)]
    pub fn remove(&mut self, index: usize) -> Output {
        self.0.remove(index)
    }
}

impl From<Vec<Output>> for Outputs {
    #[inline(always)]
    fn from(outputs: Vec<Output>) -> Self {
        Self(outputs)
    }
}

impl From<Outputs> for Vec<Output> {
    #[inline(always)]
    fn from(outputs: Outputs) -> Self {
        outputs.0
    }
}

impl IntoIterator for Outputs {
    type IntoIter = <Vec<Output> as IntoIterator>::IntoIter;
    type Item = Output;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Outputs {
    type IntoIter = <&'a Vec<Output> as IntoIterator>::IntoIter;
    type Item = &'a Output;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl std::ops::Index<usize> for Outputs {
    type Output = Output;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl std::ops::IndexMut<usize> for Outputs {
    #[inline(always)]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl FromIterator<Output> for Outputs {
    #[inline(always)]
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Output>,
    {
        Self(iter.into_iter().collect())
    }
}
