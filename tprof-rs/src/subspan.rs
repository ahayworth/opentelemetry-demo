use crate::stats::*;
use crate::trace::*;

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

#[derive(Debug, Default, Clone)]
pub struct SubspanAnalysis {
    pub length: usize,
    // pub trace_ids: Vec<TraceId>,
    pub arrows: Vec<SpanArrow>,
    pub all: HashMap<String, Stats>,
    pub norm: HashMap<String, Stats>,
    pub tail: HashMap<String, Stats>,
    pub diff: HashMap<String, Stats>,
}

pub struct SubspanProfiler {}
impl SubspanProfiler {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(group), fields(n_group = group.len()))]
    pub fn profile(tail_cutoff: f64, group: &[Trace]) -> SubspanAnalysis {
        let mut group = group.to_owned();
        group.sort_by_key(|t| t.duration);

        let mut result = SubspanAnalysis::default();
        let idx_cutoff = (tail_cutoff / 100f64) * group.len() as f64;
        let idx_cutoff = idx_cutoff.floor() as usize;

        result.length = group.len();
        // result.trace_ids = group.iter().map(|t| t.trace_id.clone()).collect();

        result.all = Self::calc_stat(&group);
        result.norm = Self::calc_stat(&group[..idx_cutoff]);
        result.tail = Self::calc_stat(&group[idx_cutoff..]);
        result.diff = Self::calc_diff(&result.norm, &result.tail);

        result
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    fn calc_diff(
        norm: &HashMap<String, Stats>,
        tail: &HashMap<String, Stats>,
    ) -> HashMap<String, Stats> {
        HashMap::from_iter(tail.iter().filter_map(|(tail_k, tail_v)| {
            norm.get(tail_k)
                .map(|norm_v| (tail_k.clone(), tail_v - norm_v))
        }))
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    fn calc_stat(traces: &[Trace]) -> HashMap<String, Stats> {
        let mut durations: HashMap<String, Vec<f64>> = HashMap::new();

        let mut to_process = VecDeque::new();
        let mut trace_arrows: HashMap<String, Vec<SpanArrow>> = HashMap::new();
        let mut trace_subspans: HashMap<String, Vec<Subspan>> = HashMap::new();
        for t in traces {
            to_process.clear();
            trace_arrows.clear();
            trace_subspans.clear();

            to_process.push_back(Rc::clone(&t.root));
            while let Some(span) = to_process.pop_front() {
                let span = span.borrow();
                trace_arrows
                    .entry(span.inner.operation_name())
                    .and_modify(|v| v.extend(span.arrows.clone()))
                    .or_insert(span.arrows.clone());
            }

            for (name, arrows) in trace_arrows.iter_mut() {
                arrows.sort_by_key(|a| match a {
                    SpanArrow::Begin { time } => (time.clone(), 0),
                    SpanArrow::Forward { time, .. } => (time.clone(), 1),
                    SpanArrow::Receive { time, .. } => (time.clone(), 2),
                    SpanArrow::Terminate { time, .. } => (time.clone(), 3),
                });

                let mut subspans = Vec::default();
                let mut prev_time = 0;
                let mut index = 0;

                'arrow_loop: for a in arrows {
                    match a {
                        SpanArrow::Forward { time, .. } | SpanArrow::Terminate { time } => {
                            subspans.push(Subspan {
                                start_time: prev_time,
                                end_time: *time,
                                id: name.clone(),
                                index,
                            });
                            index += 1;
                        }
                        _ => {} // pass
                    }
                    prev_time = match a {
                        SpanArrow::Begin { time } => *time,
                        SpanArrow::Forward { time, .. } => *time,
                        SpanArrow::Receive { time, .. } => *time,
                        SpanArrow::Terminate { time } => *time,
                    };

                    if matches!(a, SpanArrow::Terminate { .. }) {
                        break 'arrow_loop;
                    }
                }

                trace_subspans.insert(name.clone(), subspans);
            }

            for (subspan_name, subspans) in trace_subspans.iter() {
                to_process.clear();
                to_process.push_back(Rc::clone(&t.root));
                'find_span_loop: while let Some(span) = to_process.pop_front() {
                    let span = span.borrow();
                    let span_name = span.inner.operation_name();
                    if &span_name == subspan_name {
                        let span_name = format!("{span_name}FullSpan");
                        durations
                            .entry(span_name)
                            .and_modify(|v| v.push(span.inner.duration as f64))
                            .or_insert(vec![span.inner.duration as f64]);

                        break 'find_span_loop;
                    }
                }

                for subspan in subspans {
                    let name = format!("{subspan_name}{}", subspan.index);
                    let duration = (subspan.end_time - subspan.start_time) as f64;
                    durations
                        .entry(name)
                        .and_modify(|v| v.push(duration))
                        .or_insert(vec![duration]);
                }
            }
        }

        HashMap::from_iter(durations.into_iter().map(|(k, v)| (k, v.as_slice().into())))
    }
}
