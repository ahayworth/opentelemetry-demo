use crate::stats::*;
use crate::trace::*;

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

#[derive(Debug, Default, Clone)]
pub struct FunctionAnalysis {
    // pub trace_ids: Vec<TraceId>,
    pub trace_99: TraceId,
    pub trace_99_left: TraceId,
    pub trace_99_right: TraceId,
    pub operations: InnerFunctionAnalysis,
    pub self_operations: InnerFunctionAnalysis,
}

#[derive(Debug, Default, Clone)]
pub struct InnerFunctionAnalysis {
    pub all: HashMap<String, Stats>,
    pub norm: HashMap<String, Stats>,
    pub tail: HashMap<String, Stats>,
    pub diff: HashMap<String, Stats>,
}

pub struct FunctionProfiler {}
impl FunctionProfiler {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(group), fields(n_group = group.len()))]
    pub fn profile(tail_cutoff: f64, group: &[Trace]) -> FunctionAnalysis {
        let mut group = group.to_owned();
        group.sort_by_key(|t| t.duration);

        let mut result = FunctionAnalysis::default();

        let idx_cutoff = (tail_cutoff / 100f64) * group.len() as f64;
        let idx_cutoff = idx_cutoff.floor() as usize;

        let idx_99 = group.len() as f64 * 0.99f64;
        let idx_99 = idx_99.floor() as usize;

        // result.trace_ids = group.iter().map(|t| t.trace_id.clone()).collect();

        result.trace_99 = group[idx_99].trace_id.clone();
        if let Some(t) = group.get(idx_99 - 1) {
            result.trace_99_left = t.trace_id.clone();
        }
        if let Some(t) = group.get(idx_99 + 1) {
            result.trace_99_right = t.trace_id.clone();
        }

        result.operations.all = Self::calc_operation(&group);
        result.operations.norm = Self::calc_operation(&group[..idx_cutoff]);
        result.operations.tail = Self::calc_operation(&group[idx_cutoff..]);
        result.operations.diff = Self::calc_diff(&result.operations.norm, &result.operations.tail);

        result.self_operations.all = Self::calc_operation_self(&group);
        result.self_operations.norm = Self::calc_operation_self(&group[..idx_cutoff]);
        result.self_operations.tail = Self::calc_operation_self(&group[idx_cutoff..]);
        result.self_operations.diff =
            Self::calc_diff(&result.self_operations.norm, &result.self_operations.tail);

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
    fn calc_operation(traces: &[Trace]) -> HashMap<String, Stats> {
        let mut operations: HashMap<String, Vec<f64>> = HashMap::new();
        let mut to_process = VecDeque::new();
        for trace in traces {
            to_process.clear();
            to_process.push_back(Rc::clone(&trace.root));

            while let Some(span) = to_process.pop_front() {
                let span = &span.borrow();
                let duration = span.inner.duration as f64;
                operations
                    .entry(span.inner.operation_name())
                    .and_modify(|v| v.push(duration))
                    .or_insert(vec![duration]);

                for c in &span.children {
                    to_process.push_back(Rc::clone(c));
                }
            }
        }

        HashMap::from_iter(
            operations
                .into_iter()
                .map(|(operation_name, data)| (operation_name, data.as_slice().into())),
        )
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    fn calc_operation_self(traces: &[Trace]) -> HashMap<String, Stats> {
        let mut operations: HashMap<String, Vec<f64>> = HashMap::new();
        let mut to_process = VecDeque::new();
        for trace in traces {
            to_process.clear();
            to_process.push_back(Rc::clone(&trace.root));

            while let Some(span) = to_process.pop_front() {
                let mut job_counter = 0;
                let mut time_counter = 0;
                let mut prev_time = 0;

                let span = span.borrow();
                let mut arrows = span.arrows.clone();
                arrows.sort_by_key(|a| match a {
                    SpanArrow::Begin { time } => (time.clone(), 0),
                    SpanArrow::Forward { time, .. } => (time.clone(), 1),
                    SpanArrow::Receive { time, .. } => (time.clone(), 2),
                    SpanArrow::Terminate { time, .. } => (time.clone(), 3),
                });

                'arrow_loop: for arrow in &span.arrows {
                    match arrow {
                        SpanArrow::Begin { time } => prev_time = *time,
                        SpanArrow::Forward { time, .. } => {
                            if job_counter == 0 {
                                time_counter += time - prev_time;
                            }
                            job_counter += 1;
                        }
                        SpanArrow::Receive { time, .. } => {
                            if job_counter == 1 {
                                prev_time = *time;
                            }
                            job_counter -= 1;
                        }
                        SpanArrow::Terminate { time } => {
                            if job_counter == 0 {
                                time_counter += time - prev_time;
                            }
                            break 'arrow_loop;
                        }
                    }
                }

                let time_counter = time_counter as f64;
                operations
                    .entry(span.inner.operation_name())
                    .and_modify(|v| v.push(time_counter))
                    .or_insert(vec![time_counter]);

                for c in &span.children {
                    to_process.push_back(Rc::clone(c));
                }
            }
        }

        HashMap::from_iter(
            operations
                .into_iter()
                .map(|(operation_name, data)| (operation_name, data.as_slice().into())),
        )
    }
}
