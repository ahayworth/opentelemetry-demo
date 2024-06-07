use anyhow::Result;
use sha2::{Digest, Sha256};
use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*, EnvFilter};

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::time::Instant;

mod child_diff;
mod function_analysis;
mod stats;
mod subspan;
mod trace;

use child_diff::*;
use function_analysis::*;
use subspan::*;
use trace::*;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            fmt::layer().with_span_events(fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE),
        )
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let tail_cutoff: f64 = 90f64;
    let tail_threshold: f64 = 3f64;
    let max_l1_results = 10;
    let max_l2_3_4_results = 2;

    tracing::info!("Querying clickhouse...");
    let clickhouse_start = Instant::now();
    let (spans, traces) = get_spans().await?;
    tracing::info!(
        n_spans = spans.len(),
        n_traces = traces.len(),
        duration = ?clickhouse_start.elapsed(),
        "Retrieved spans from clickhouse and constructed trace objects"
    );

    tracing::info!(tail_cutoff, max_l1_results, max_l2_3_4_results, "Running tprof analysis...");
    let analysis_start = Instant::now();
    let base_group: Vec<Trace> = traces.values().cloned().collect();
    let mut results = Vec::default();
    for (l1_name, l1_g) in NoopGrouper::group(&base_group) {
        tracing::info!(
            n_traces = l1_g.len(),
            l1_name,
            "Generating layer one result"
        );
        let mut l1_results = LayerOneResult {
            inner: FunctionProfiler::profile(tail_cutoff, &l1_g),
            children: HashMap::default(),
        };

        for (l2_name, l2_g) in ServiceGrouper::group(&l1_g) {
            tracing::info!(
                n_traces = l2_g.len(),
                l2_name,
                "Generating layer two result"
            );
            let mut l2_results = LayerTwoResult {
                inner: FunctionProfiler::profile(tail_cutoff, &l2_g),
                group: l2_name.clone(),
                children: Vec::default(),
            };

            for (l3_name, l3_g) in TraceStructureGrouper::group(&l2_g) {
                tracing::debug!(
                    n_traces = l3_g.len(),
                    l3_name,
                    "Generating layer three result"
                );
                let mut l3_results = LayerThreeResult {
                    inner: ChildDiffProfiler::profile(tail_cutoff, &l3_g),
                    group: l3_name,
                    children: Vec::default(),
                };

                // SpanArrowGrouper modifies spans in-place! Mainly because I can't be arsed to do
                // any kind of "deep_clone()" on the trace tree structure! Beware! If you're
                // extending this code, that's probably something you should know!
                for (l4_name, l4_g) in SpanArrowGrouper::group(&l3_g) {
                    tracing::debug!(
                        n_traces = l4_g.len(),
                        l4_name,
                        "Generating layer four result"
                    );
                    let l4_result = LayerFourResult {
                        inner: SubspanProfiler::profile(tail_cutoff, &l4_g),
                        group: l4_name,
                    };

                    l3_results.children.push(l4_result);
                }

                l2_results.children.push(l3_results);
            }

            l1_results.children.insert(l2_name, l2_results);
        }

        results.push(l1_results);
    }
    tracing::info!(duration = ?analysis_start.elapsed(), "Finished analysis");

    process_l1(&results, max_l1_results, max_l2_3_4_results, tail_threshold)?;

    Ok(())
}

fn process_l1(l1_results: &[LayerOneResult], max_l1: usize, max_l2_l3_l4: usize, tail_threshold: f64) -> Result<()> {
    // TODO: The whole "group by nothing for a single l1 group" just feels like annoying nonsense.
    let mut worst_l1 = l1_results[0].inner.self_operations.all.iter().collect::<Vec<_>>();

    // NB: We're sorting in reverse!
    worst_l1.sort_by(|(_, a), (_, b)| (b.count as f64 * b.mean).total_cmp(&(a.count as f64 * a.mean)));
    let worst_l1 = worst_l1.iter().map(|(k, _)| k).take(max_l1).collect::<Vec<_>>();

    tracing::info!(worst_l1 = ?worst_l1, "Found top {max_l1} worst spans from layer 1");

    for l1_servop in worst_l1 {
        tracing::info!(l1_servop, "Looking for most impacted request kinds that contained this kind of span");
        process_l2(&l1_results[0].children, l1_servop, max_l2_l3_l4, tail_threshold)?;
    }

    Ok(())
}

fn process_l2(l2_results: &HashMap<String, LayerTwoResult>, l1_servop: &str, max_l2_l3_l4: usize, tail_threshold: f64) -> Result<()> {
        // TODO: is there a better way? This feels awful.
        let mut l2_tail_keys = Vec::default();
        let mut l2_all_keys = Vec::default();

        for (k, r) in l2_results.iter() {
            if r.inner.self_operations.tail.contains_key(l1_servop) {
                l2_tail_keys.push(k.clone());
            } else if r.inner.self_operations.all.contains_key(l1_servop) {
                l2_all_keys.push(k.clone());
            }
        }

        // NB: Sort is in reverse!
        l2_tail_keys.sort_by(|a, b| {
            let a = &l2_results[a].inner.self_operations.tail[l1_servop];
            let b = &l2_results[b].inner.self_operations.tail[l1_servop];
            (b.count as f64 * b.mean).total_cmp(&(a.count as f64 * a.mean))
        });
        l2_all_keys.sort_by(|a, b| {
            let a = &l2_results[a].inner.self_operations.all[l1_servop];
            let b = &l2_results[b].inner.self_operations.all[l1_servop];
            (b.count as f64 * b.mean).total_cmp(&(a.count as f64 * a.mean))
        });

        let mut found_l2 = Vec::default();
        for l2_req_kind in l2_tail_keys {
            println!("foo {l2_req_kind}");
            if found_l2.len() < max_l2_l3_l4 {
                if let Some(norm_stats) = l2_results[l2_req_kind.as_str()].inner.self_operations.norm.get(l1_servop) {
                    let tail_stats = &l2_results[l2_req_kind.as_str()].inner.self_operations.tail[l1_servop];
                    if tail_stats.mean > (tail_threshold * norm_stats.mean) {
                        tracing::info!(l2_req_kind, "Found impacted tail request");

                        process_l3(&l2_results[l2_req_kind.as_str()].children, max_l2_l3_l4, l1_servop, true)?;
                        found_l2.push(l2_req_kind.clone());
                    } else {
                        tracing::info!(tail_stats.mean, norm_stats.mean, "Found matching tail:norm stats but tail mean wasn't bad enough?");
                    }
                // } else {
                //     let norm = &results[0].children[l2_req_kind.as_str()].inner.self_operations.norm.keys().collect::<Vec<_>>();
                //     tracing::info!(?norm, "Couldn't find matching norm stats");
                }
            }
        }

        for l2_req_kind in l2_all_keys {
            println!("bar {l2_req_kind}");
            // Skip over things we've already analyzed as a "tail bug" above
            if found_l2.len() < max_l2_l3_l4 && !found_l2.contains(&l2_req_kind) {
                tracing::info!(l2_req_kind, "Found impacted all request");
                process_l3(&l2_results[l2_req_kind.as_str()].children, max_l2_l3_l4, l1_servop, false)?;
                found_l2.push(l2_req_kind.clone());
            }
        }

    Ok(())
}

#[derive(Debug)]
pub enum ChildDiffKind {
    NoChild,
    ChildStart,
    EndDiff,
}

fn process_l3(l3_results: &[LayerThreeResult], max_l2_l3_l4: usize, l1_servop: &str, is_tail: bool) -> Result<()> {
    // process the l3 children of this result.
    // find the child diff stats for the span
    // in question in each l3 child group.
    // (take care with tail vs non tail)
    // sort by the "worst" child stats
    // limit l2_3_4_results
    // analyze l4 based on:
    // - are there no child diffs for that span? if so thats full span
    // suspicious
    // - are there any end diffs? that's a last part long
    // - otherwise its a child starts late
    // then figure out how l4 owrks
    let mut to_process = VecDeque::new();
    let mut found = Vec::default();
    for l3_result in l3_results {
        let inner_diff = if is_tail {
            if l3_result.inner.tail.is_none() {
                tracing::info!("Tried to process l3 tail diff for {}, but there is no actual tail child diff stats to process!", l3_result.group);
                continue;
            }

            l3_result.inner.tail.clone().unwrap()
        } else {
            l3_result.inner.overall.clone()
        };

        to_process.clear();
        to_process.push_back((Rc::clone(&inner_diff.root), Vec::default()));

        while let Some((cds_rc, path)) = to_process.pop_front() {
            let cds = cds_rc.borrow();
            // tracing::info!(l1_servop, cds.operation_name, ?path);
            if cds.operation_name == l1_servop {
                if cds.children.is_empty() {
                    found.push((
                        ChildDiffKind::NoChild,
                        crate::stats::Stats::default(),
                        Rc::clone(&cds_rc),
                        l3_result.clone(),
                        path.clone(),
                    ));
                } else {
                    found.push((
                        ChildDiffKind::EndDiff,
                        cds.durations_after_last_child.clone(),
                        Rc::clone(&cds_rc),
                        l3_result.clone(),
                        path.clone(),
                    ));

                    for c in &cds.child_start_offsets {
                        found.push((
                            ChildDiffKind::ChildStart,
                            c.clone(),
                            Rc::clone(&cds_rc),
                            l3_result.clone(),
                            path.clone(),
                        ));
                    }
                }
            }

            if !cds.children.is_empty() {
                let mut path = path.clone();
                path.push(cds.operation_name.clone());

                for c in &cds.children {
                    to_process.push_back((Rc::clone(c), path.clone()));
                }
            }
        }
    }

    // I think this is right?
    // NB: Sort in reverse!
    found.sort_by(|(a_kind, a_stats, a_cds, _, _), (b_kind, b_stats, b_cds, _, _)| {
        let a = match a_kind {
            ChildDiffKind::NoChild => a_stats.mean,
            ChildDiffKind::EndDiff => a_stats.mean,
            ChildDiffKind::ChildStart => {
                a_cds.borrow().children.len() as f64 * a_stats.mean
            }
        };

        let b = match b_kind {
            ChildDiffKind::NoChild => b_stats.mean,
            ChildDiffKind::EndDiff => b_stats.mean,
            ChildDiffKind::ChildStart => {
                b_cds.borrow().children.len() as f64 * b_stats.mean
            }
        };

        b.total_cmp(&a)
    });

    // for x in found.iter().take(max_l2_l3_l4) {
    //     tracing::info!(?x);
    // }

    Ok(())
}

trait Grouper {
    fn group(traces: &[Trace]) -> Vec<(String, Vec<Trace>)>;
}

struct NoopGrouper {}
impl Grouper for NoopGrouper {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(traces), fields(n_traces = traces.len()))]
    fn group(traces: &[Trace]) -> Vec<(String, Vec<Trace>)> {
        vec![("base".into(), traces.to_vec())]
    }
}

struct ServiceGrouper {}
impl Grouper for ServiceGrouper {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(traces), fields(n_traces = traces.len()))]
    fn group(traces: &[Trace]) -> Vec<(String, Vec<Trace>)> {
        let mut groups: HashMap<String, Vec<Trace>> = HashMap::new();
        for t in traces {
            let service_name = t.root.borrow().inner.service_name.clone();
            groups
                .entry(service_name)
                .and_modify(|v| v.push(t.clone()))
                .or_insert(vec![t.clone()]);
        }

        groups.into_iter().collect()
    }
}

struct TraceStructureGrouper {}
impl Grouper for TraceStructureGrouper {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(traces), fields(n_traces = traces.len()))]
    fn group(traces: &[Trace]) -> Vec<(String, Vec<Trace>)> {
        let mut groups: HashMap<String, Vec<Trace>> = HashMap::new();
        let mut hasher = Sha256::new();
        let mut to_process = VecDeque::new();

        for t in traces {
            to_process.clear();
            to_process.push_back(Rc::clone(&t.root));

            while let Some(span) = to_process.pop_front() {
                let span = span.borrow();
                hasher.update(&span.inner.operation_name());

                let mut children = span.children.clone();
                children.sort_by_key(|cs| {
                    let cs = cs.borrow();
                    (
                        cs.inner.operation_name(),
                        cs.children.len()
                    )
                });
                for cs in children {
                    hasher.update(&cs.borrow().inner.operation_name());
                    to_process.push_back(Rc::clone(&cs));
                }
            }

            let hash = format!("{:x}", hasher.finalize_reset());
            groups
                .entry(hash)
                .and_modify(|v| v.push(t.clone()))
                .or_insert(vec![t.clone()]);
        }

        groups.into_iter().collect()
    }
}

struct SpanArrowGrouper {}
impl Grouper for SpanArrowGrouper {
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(traces), fields(n_traces = traces.len()))]
    fn group(traces: &[Trace]) -> Vec<(String, Vec<Trace>)> {
        let mut groups: HashMap<String, Vec<Trace>> = HashMap::new();
        let mut to_process = VecDeque::new();
        let mut function_counts: HashMap<String, usize> = HashMap::new();
        let mut new_span_names: HashMap<SpanId, String> = HashMap::new();
        let mut hasher = Sha256::new();

        for t in traces {
            to_process.clear();
            function_counts.clear();
            new_span_names.clear();

            to_process.push_back((Rc::clone(&t.root), Vec::default()));
            while let Some((span, path)) = to_process.pop_front() {
                let mut span = span.borrow_mut();
                let old_name = span.inner.operation_name();
                let count = function_counts
                    .entry(old_name.clone())
                    .and_modify(|v| *v += 1)
                    .or_insert(0);

                let new_name = if *count == 0 {
                    old_name
                } else {
                    format!("{old_name}[{count}]")
                };

                let mut new_path = path.clone();
                new_path.push(new_name);

                let new_name = new_path.join("~");
                new_span_names.insert(span.inner.span_id.clone(), new_name.clone());

                span.inner.service_name = new_name;
                span.inner.span_name = "".into();

                let mut children = span.children.clone();
                children.sort_by_key(|c| c.borrow().inner.start_timestamp);
                for c in children {
                    to_process.push_back((Rc::clone(&c), new_path.clone()));
                }
            }

            to_process.clear();
            to_process.push_back((Rc::clone(&t.root), Vec::default()));
            while let Some((span, _)) = to_process.pop_front() {
                let mut span = span.borrow_mut();
                hasher.update(span.inner.operation_name());

                for a in span.arrows.iter_mut() {
                    match a {
                        SpanArrow::Forward { parent, child, .. }
                        | SpanArrow::Receive { parent, child, .. } => {
                            *parent = new_span_names[parent].clone();
                            *child = new_span_names[child].clone();
                        }
                        _ => {} // pass
                    }
                }

                // The python code sorts by (time, arrow kind)...
                // but rust by default (with #[derive(Ord)]) will
                // actually sort by arrow kind first. So we do
                // some nonsense here to overcome that.
                let mut arrows = span.arrows.clone();
                arrows.sort_by_key(|a| match a {
                    SpanArrow::Begin { time } => (time.clone(), 0),
                    SpanArrow::Forward { time, .. } => (time.clone(), 1),
                    SpanArrow::Receive { time, .. } => (time.clone(), 2),
                    SpanArrow::Terminate { time, .. } => (time.clone(), 3),
                });

                for a in arrows {
                    match a {
                        SpanArrow::Forward { parent, child, .. }
                        | SpanArrow::Receive { parent, child, .. } => {
                            hasher.update(parent);
                            hasher.update(child);
                        }
                        // We don't actually need to do anything for
                        // begin/terminate arrows, even though the
                        // python code has them. We don't use them
                        // to generate anything interesting about
                        // the grouping, and every span will have
                        // a begin/terminate arrow. :shrug:
                        _ => {}
                    }
                }

                let mut children = span.children.clone();
                children.sort_by_key(|c| c.borrow().inner.start_timestamp);
                for c in children {
                    to_process.push_back((Rc::clone(&c), Vec::default()));
                }
            }

            let hash = format!("{:x}", hasher.finalize_reset());
            groups
                .entry(hash)
                .and_modify(|v| v.push(t.clone()))
                .or_insert(vec![t.clone()]);
        }

        groups.into_iter().collect()
    }
}

#[derive(Debug, Default, Clone)]
struct LayerOneResult {
    inner: FunctionAnalysis,
    children: HashMap<String, LayerTwoResult>,
}

#[derive(Debug, Default, Clone)]
struct LayerTwoResult {
    group: String,
    inner: FunctionAnalysis,
    children: Vec<LayerThreeResult>,
}

#[derive(Default, Debug, Clone)]
struct LayerThreeResult {
    group: String,
    inner: ChildDiffAnalysis,
    children: Vec<LayerFourResult>,
}

#[derive(Default, Clone)]
struct LayerFourResult {
    group: String,
    inner: SubspanAnalysis,
}
impl std::fmt::Debug for LayerFourResult {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        Ok(())
    }
}
