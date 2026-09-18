use std::{borrow::Borrow, cmp::Ordering};

use borsh::BorshSerialize;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::OutPoint;
use crate::types::hashes::{self, Hash, InputsMerkleRoot};

/// Internal node of a CBMT
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CbmtNode {
    /// Commitment to child nodes or leaf value
    commitment: Hash,
    /// CBT index. `CbmtNode` orders by this, and nothing else.
    index: usize,
}

impl PartialOrd for CbmtNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CbmtNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index.cmp(&other.index)
    }
}

/// Marker type for merging branch commitments
struct Merge;

impl merkle_cbt::merkle_tree::Merge for Merge {
    type Item = CbmtNode;

    fn merge(lnode: &Self::Item, rnode: &Self::Item) -> Self::Item {
        assert_eq!(lnode.index + 1, rnode.index);
        let index = (lnode.index - 1) / 2;
        let commitment = hashes::hash(&(&lnode.commitment, &rnode.commitment));
        CbmtNode { commitment, index }
    }
}

/// Complete binary merkle tree
type Cbmt = merkle_cbt::CBMT<CbmtNode, Merge>;

#[derive(BorshSerialize, Clone, Debug, Deserialize, Serialize, ToSchema)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Inputs(pub Vec<OutPoint>);

impl Inputs {
    #[inline(always)]
    pub fn as_slice(&self) -> &[OutPoint] {
        self.0.as_slice()
    }

    #[inline(always)]
    pub fn extend<I>(&mut self, inputs: I)
    where
        I: IntoIterator,
        I::Item: Borrow<OutPoint>,
    {
        self.0
            .extend(inputs.into_iter().map(|input| *input.borrow()))
    }

    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&OutPoint> {
        self.0.get(index)
    }

    #[inline(always)]
    pub fn last(&self) -> Option<&OutPoint> {
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
    pub fn iter(&self) -> std::slice::Iter<'_, OutPoint> {
        self.0.iter()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn push(&mut self, input: OutPoint) {
        self.0.push(input)
    }

    #[inline(always)]
    pub fn remove(&mut self, index: usize) -> OutPoint {
        self.0.remove(index)
    }

    fn merkle_leaves(&self) -> Vec<CbmtNode> {
        let n_inputs = self.len();
        self.iter()
            .enumerate()
            .map(|(index, outpoint)| CbmtNode {
                commitment: hashes::hash(outpoint),
                index: (index + n_inputs) - 1,
            })
            .collect()
    }

    pub(crate) fn compute_merkle_root(&self) -> InputsMerkleRoot {
        let CbmtNode { commitment, .. } =
            Cbmt::build_merkle_root(self.merkle_leaves().as_slice());
        commitment.into()
    }
}

impl Default for Inputs {
    #[inline(always)]
    fn default() -> Self {
        Self(Vec::default())
    }
}

impl From<Vec<OutPoint>> for Inputs {
    #[inline(always)]
    fn from(inputs: Vec<OutPoint>) -> Self {
        Self(inputs)
    }
}

impl From<Inputs> for Vec<OutPoint> {
    #[inline(always)]
    fn from(inputs: Inputs) -> Self {
        inputs.0
    }
}

impl IntoIterator for Inputs {
    type IntoIter = <Vec<OutPoint> as IntoIterator>::IntoIter;
    type Item = OutPoint;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Inputs {
    type IntoIter = <&'a Vec<OutPoint> as IntoIterator>::IntoIter;
    type Item = &'a OutPoint;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl std::ops::Index<usize> for Inputs {
    type Output = OutPoint;

    #[inline(always)]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl FromIterator<OutPoint> for Inputs {
    #[inline(always)]
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = OutPoint>,
    {
        Self(iter.into_iter().collect())
    }
}
