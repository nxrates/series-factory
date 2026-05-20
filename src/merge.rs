//! Streaming k-way merge over per-source tick channels.
//!
//! Each source emits ticks in timestamp-ascending order within a batch and
//! across batches (CSV parsers + `par_sort_unstable` in `download_and_convert`
//! enforce that). `MergedTickStream` composes those per-source streams into a
//! single globally sorted stream by maintaining a min-heap keyed on the head
//! tick of each source's current batch. Memory is bounded by the number of
//! in-flight batches + the batch size — no full-run sort buffer.

use crate::types::TickFrame;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use tokio::sync::mpsc;

/// Merge many per-source `Receiver<Vec<TickFrame>>` streams into a single
/// globally timestamp-ordered stream of ticks.
pub struct MergedTickStream {
    sources: Vec<SourceState>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
}

struct SourceState {
    rx: mpsc::Receiver<Vec<TickFrame>>,
    batch: Vec<TickFrame>,
    cursor: usize,
    exhausted: bool,
}

#[derive(PartialEq, Eq)]
struct HeapEntry {
    ts: i64,
    source_idx: usize,
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Tie-break on source_idx so equal-timestamp ticks emit in a stable,
        // deterministic order across runs.
        self.ts.cmp(&other.ts).then(self.source_idx.cmp(&other.source_idx))
    }
}

impl MergedTickStream {
    /// Prime the heap with each source's first-available tick.
    pub async fn new(receivers: Vec<mpsc::Receiver<Vec<TickFrame>>>) -> Self {
        let mut sources: Vec<SourceState> = receivers
            .into_iter()
            .map(|rx| SourceState {
                rx,
                batch: Vec::new(),
                cursor: 0,
                exhausted: false,
            })
            .collect();

        let mut heap = BinaryHeap::with_capacity(sources.len());
        for idx in 0..sources.len() {
            if let Some(ts) = prime_source(&mut sources[idx]).await {
                heap.push(Reverse(HeapEntry { ts, source_idx: idx }));
            }
        }
        Self { sources, heap }
    }

    /// Yield the next globally-smallest tick across all live sources.
    pub async fn next(&mut self) -> Option<TickFrame> {
        let Reverse(HeapEntry { source_idx, .. }) = self.heap.pop()?;
        let src = &mut self.sources[source_idx];
        let tick = src.batch[src.cursor];
        src.cursor += 1;

        if src.cursor >= src.batch.len() {
            if let Some(ts) = prime_source(src).await {
                self.heap.push(Reverse(HeapEntry { ts, source_idx }));
            }
        } else {
            let next_ts = src.batch[src.cursor].timestamp_ms();
            self.heap.push(Reverse(HeapEntry { ts: next_ts, source_idx }));
        }
        Some(tick)
    }
}

/// Advance a source past its exhausted current batch by pulling the next
/// one. Returns the timestamp of the new head tick, or `None` when the
/// channel closes with no more data.
async fn prime_source(src: &mut SourceState) -> Option<i64> {
    if src.exhausted {
        return None;
    }
    loop {
        if src.cursor < src.batch.len() {
            return Some(src.batch[src.cursor].timestamp_ms());
        }
        match src.rx.recv().await {
            Some(batch) if !batch.is_empty() => {
                src.batch = batch;
                src.cursor = 0;
            }
            Some(_) => continue,
            None => {
                src.exhausted = true;
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitch::Tick;

    /// MITCH epoch is 2010-01-01 UTC; all test timestamps must be after it or
    /// `from_epoch_ms` saturates to 0 and order collapses.
    const T0: i64 = 1_700_000_000_000; // 2023-11-14 UTC

    fn frame(provider_id: u16, offset_ms: i64) -> TickFrame {
        TickFrame::new(
            provider_id,
            mitch::timestamp::from_epoch_ms(T0 + offset_ms),
            Tick::new_unchecked(1, 100.0, 100.01, 1, 1),
        )
    }

    fn offsets(ticks: &[TickFrame]) -> Vec<i64> {
        ticks.iter().map(|t| t.timestamp_ms() - T0).collect()
    }

    /// A source that emits its second (earlier-timestamp) batch AFTER a
    /// source that sent only later-timestamp ticks must still produce
    /// globally-sorted output through the merge.
    #[tokio::test]
    async fn merge_preserves_global_order_under_skew() {
        let (tx_a, rx_a) = mpsc::channel::<Vec<TickFrame>>(4);
        let (tx_b, rx_b) = mpsc::channel::<Vec<TickFrame>>(4);

        // Source B pushes LATE (+200, +300) FIRST.
        tx_b.send(vec![frame(2, 200), frame(2, 300)]).await.unwrap();
        // Source A then pushes EARLY (+10, +50) and then some later ones (+150).
        tx_a.send(vec![frame(1, 10), frame(1, 50)]).await.unwrap();
        tx_a.send(vec![frame(1, 150)]).await.unwrap();
        drop(tx_a);
        drop(tx_b);

        let mut stream = MergedTickStream::new(vec![rx_a, rx_b]).await;
        let mut out = Vec::new();
        while let Some(t) = stream.next().await {
            out.push(t);
        }
        // Emission offsets (subtracting T0) must be globally sorted, regardless
        // of arrival order.
        assert_eq!(offsets(&out), vec![10, 50, 150, 200, 300]);
    }

    /// Equal timestamps tie-break on source_idx for deterministic ordering.
    #[tokio::test]
    async fn merge_stable_on_ties() {
        let (tx_a, rx_a) = mpsc::channel::<Vec<TickFrame>>(4);
        let (tx_b, rx_b) = mpsc::channel::<Vec<TickFrame>>(4);
        tx_a.send(vec![frame(1, 100)]).await.unwrap();
        tx_b.send(vec![frame(2, 100)]).await.unwrap();
        drop(tx_a);
        drop(tx_b);
        let mut stream = MergedTickStream::new(vec![rx_a, rx_b]).await;
        let first = stream.next().await.unwrap();
        let second = stream.next().await.unwrap();
        assert!(stream.next().await.is_none());
        assert_eq!(first.provider_id(), 1);
        assert_eq!(second.provider_id(), 2);
    }

    /// A source that never sends anything (closed channel with no batches)
    /// must not block the merge.
    #[tokio::test]
    async fn merge_handles_empty_source() {
        let (tx_a, rx_a) = mpsc::channel::<Vec<TickFrame>>(4);
        let (tx_empty, rx_empty) = mpsc::channel::<Vec<TickFrame>>(4);
        tx_a.send(vec![frame(1, 10), frame(1, 20)]).await.unwrap();
        drop(tx_a);
        drop(tx_empty);
        let mut stream = MergedTickStream::new(vec![rx_a, rx_empty]).await;
        let mut out = Vec::new();
        while let Some(t) = stream.next().await {
            out.push(t);
        }
        assert_eq!(out.len(), 2);
        assert!(offsets(&out).windows(2).all(|w| w[0] <= w[1]));
    }
}
