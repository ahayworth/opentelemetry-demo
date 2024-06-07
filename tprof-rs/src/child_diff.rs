use crate::stats::*;
use crate::trace::*;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

#[derive(Debug, Default, Clone)]
pub struct ChildDiffAnalysis {
    pub length: usize,
    // pub trace_ids: Vec<TraceId>,
    pub overall: InnerChildDiffAnalysis,
    pub norm: Option<InnerChildDiffAnalysis>,
    pub tail: Option<InnerChildDiffAnalysis>,
    pub diff: Option<InnerChildDiffAnalysis>,
}

#[derive(Debug, Default, Clone)]
pub struct InnerChildDiffAnalysis {
    pub root: Rc<RefCell<ChildDiffStats>>,
}

#[derive(Debug, Default, Clone)]
pub struct ChildDiffStats {
    pub operation_name: String,
    pub span_duration: Stats,
    pub child_start_offsets: Vec<Stats>,
    pub durations_after_last_child: Stats,
    pub children: Vec<Rc<RefCell<ChildDiffStats>>>,
}

#[derive(Debug, Default, Clone)]
pub struct ChildDiffData {
    pub operation_name: String,
    pub span_duration: Vec<f64>,
    pub child_start_offsets: Vec<Vec<f64>>,
    pub durations_after_last_child: Vec<f64>,
    pub children: Vec<Rc<RefCell<ChildDiffData>>>,
}

pub struct ChildDiffProfiler {}
impl ChildDiffProfiler {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(group), fields(n_group = group.len()))]
    pub fn profile(tail_cutoff: f64, group: &[Trace]) -> ChildDiffAnalysis {
        let mut group = group.to_owned();
        group.sort_by_key(|t| t.duration);

        let mut result = ChildDiffAnalysis::default();
        result.length = group.len();

        let idx_cutoff = (tail_cutoff / 100f64) * group.len() as f64;
        let idx_cutoff = idx_cutoff.floor() as usize;

        let overall = {
            let structure = &group
                .iter()
                .map(|t| Self::generate_structure(t))
                .collect::<Vec<_>>();
            let structure = Self::reduce_structures(&structure);

            Self::transmute_structure(structure)
        };

        result.overall = InnerChildDiffAnalysis { root: overall };

        if group.len() > 1 {
            let norm = {
                let structure = &group[..idx_cutoff]
                    .iter()
                    .map(|t| Self::generate_structure(t))
                    .collect::<Vec<_>>();
                let structure = Self::reduce_structures(&structure);

                Self::transmute_structure(structure)
            };

            let tail = {
                let structure = &group[idx_cutoff..]
                    .iter()
                    .map(|t| Self::generate_structure(t))
                    .collect::<Vec<_>>();
                let structure = Self::reduce_structures(&structure);

                Self::transmute_structure(structure)
            };

            result.diff = Some(InnerChildDiffAnalysis {
                root: Self::calc_diff(&norm, &tail),
            });

            result.norm = Some(InnerChildDiffAnalysis { root: norm });
            result.tail = Some(InnerChildDiffAnalysis { root: tail });
        }

        result
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    fn calc_diff(
        norm: &Rc<RefCell<ChildDiffStats>>,
        tail: &Rc<RefCell<ChildDiffStats>>,
    ) -> Rc<RefCell<ChildDiffStats>> {
        let mut to_process = VecDeque::new();
        let new_structure = Rc::new(RefCell::new(ChildDiffStats::default()));
        to_process.push_back((Rc::clone(norm), Rc::clone(tail), Rc::clone(&new_structure)));
        while let Some((norm_cds, tail_cds, new_cds)) = to_process.pop_front() {
            let norm_cds = norm_cds.borrow();
            let tail_cds = tail_cds.borrow();
            let mut new_cds = new_cds.borrow_mut();

            new_cds.operation_name = norm_cds.operation_name.clone();
            new_cds.span_duration = &norm_cds.span_duration - &tail_cds.span_duration;
            new_cds.child_start_offsets = norm_cds
                .child_start_offsets
                .iter()
                .zip(tail_cds.child_start_offsets.iter())
                .map(|(n, t)| n - t)
                .collect();
            new_cds.durations_after_last_child =
                &norm_cds.durations_after_last_child - &tail_cds.durations_after_last_child;

            for (nc, tc) in norm_cds.children.iter().zip(tail_cds.children.iter()) {
                let new_child = Rc::new(RefCell::new(ChildDiffStats::default()));
                to_process.push_back((Rc::clone(nc), Rc::clone(tc), Rc::clone(&new_child)));
                new_cds.children.push(new_child);
            }
        }

        new_structure
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    fn transmute_structure(structure: Rc<RefCell<ChildDiffData>>) -> Rc<RefCell<ChildDiffStats>> {
        let mut to_process = VecDeque::new();
        let new_structure = Rc::new(RefCell::new(ChildDiffStats::default()));
        to_process.push_back((Rc::clone(&structure), Rc::clone(&new_structure)));

        while let Some((cdd, cds)) = to_process.pop_front() {
            let cdd = cdd.borrow();
            let mut cds = cds.borrow_mut();
            cds.operation_name = cdd.operation_name.clone();
            cds.span_duration = cdd.span_duration.as_slice().into();
            cds.child_start_offsets = cdd
                .child_start_offsets
                .iter()
                .map(|v| v.as_slice().into())
                .collect();
            cds.durations_after_last_child = cdd.durations_after_last_child.as_slice().into();

            for cdd_child in cdd.children.iter() {
                let new_cds = Rc::new(RefCell::new(ChildDiffStats::default()));
                to_process.push_back((Rc::clone(cdd_child), Rc::clone(&new_cds)));
                cds.children.push(new_cds);
            }
        }

        new_structure
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all, fields(n_structures = structures.len()))]
    fn reduce_structures(structures: &[Rc<RefCell<ChildDiffData>>]) -> Rc<RefCell<ChildDiffData>> {
        let mut to_process = VecDeque::new();

        let output = structures
            .iter()
            .reduce(|acc, s| {
                to_process.clear();
                to_process.push_back((Rc::clone(acc), Rc::clone(s)));

                while let Some((base, other)) = to_process.pop_front() {
                    let mut base = base.borrow_mut();
                    let mut other = other.borrow_mut();

                    if base.operation_name != other.operation_name {
                        panic!("{} != {}", base.operation_name, other.operation_name);
                    }

                    base.span_duration.append(&mut other.span_duration);
                    for (i, base_offsets) in base.child_start_offsets.iter_mut().enumerate() {
                        base_offsets.append(&mut other.child_start_offsets[i]);
                    }
                    base.durations_after_last_child
                        .append(&mut other.durations_after_last_child);

                    for (bc, oc) in base.children.iter().zip(other.children.iter()) {
                        to_process.push_back((Rc::clone(bc), Rc::clone(oc)));
                    }
                }

                acc
            })
            .unwrap();

        Rc::clone(output)
    }

    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    fn generate_structure(trace: &Trace) -> Rc<RefCell<ChildDiffData>> {
        let mut to_process = VecDeque::new();
        let root_cdd = Rc::new(RefCell::new(ChildDiffData::default()));
        to_process.push_back((Rc::clone(&trace.root), Rc::clone(&root_cdd)));

        while let Some((span, cdd)) = to_process.pop_front() {
            let span = span.borrow();
            let mut cdd = cdd.borrow_mut();

            if cdd.operation_name.is_empty() {
                cdd.operation_name = span.inner.operation_name();
            }

            cdd.span_duration.push(span.inner.duration as f64);

            let mut children = span.children.clone();
            children.sort_by_key(|cs| {
                let cs = cs.borrow();
                (
                    cs.inner.operation_name(),
                    cs.children.len(),
                    cs.inner.start_timestamp,
                )
            });
            let children_len = children.len();
            if children_len > 0 && cdd.child_start_offsets.is_empty() {
                cdd.child_start_offsets = vec![Vec::default(); children_len];
            }
            let mut prev_time = span.inner.start_timestamp;
            for (i, cs) in children.iter().enumerate() {
                let new_cdd = Rc::new(RefCell::new(ChildDiffData::default()));
                cdd.children.push(Rc::clone(&new_cdd));
                to_process.push_back((Rc::clone(cs), new_cdd));

                let cs = cs.borrow();
                cdd.child_start_offsets[i].push((cs.inner.start_timestamp - prev_time) as f64);
                prev_time = cs.inner.start_timestamp;

                if i == (children_len - 1) {
                    cdd.durations_after_last_child
                        .push((span.inner.end_timestamp - cs.inner.end_timestamp) as f64);
                }
            }
        }

        root_cdd
    }
}
