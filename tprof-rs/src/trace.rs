use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

pub type SpanId = String;
pub type TraceId = String;

#[derive(Debug, Clone)]
pub struct Trace {
    pub trace_id: TraceId,
    pub duration: i64,
    pub root: Rc<RefCell<Span>>,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum SpanArrow {
    Begin {
        time: i64,
    },
    Forward {
        time: i64,
        parent: SpanId,
        child: SpanId,
    },
    Receive {
        time: i64,
        parent: SpanId,
        child: SpanId,
    },
    Terminate {
        time: i64,
    },
}

#[derive(Debug, Clone)]
pub struct Span {
    pub inner: ClickhouseSpan,
    pub arrows: Vec<SpanArrow>,
    pub children: Vec<Rc<RefCell<Span>>>,
}

#[derive(Debug, Default, Clone)]
pub struct Subspan {
    pub start_time: i64,
    pub end_time: i64,
    pub index: usize,
    pub id: String,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct ClickhouseSpan {
    #[serde(rename = "TraceId")]
    pub trace_id: TraceId,

    #[serde(rename = "SpanId")]
    pub span_id: SpanId,

    #[serde(rename = "ParentSpanId")]
    pub parent_span_id: SpanId,

    #[serde(rename = "StartTimestamp")]
    pub start_timestamp: i64,

    #[serde(rename = "EndTimestamp")]
    pub end_timestamp: i64,

    #[serde(rename = "Duration")]
    pub duration: i64,

    #[serde(rename = "SpanName")]
    pub span_name: String,

    #[serde(rename = "SpanKind")]
    pub span_kind: String,

    #[serde(rename = "ServiceName")]
    pub service_name: String,
}

impl ClickhouseSpan {
    pub fn operation_name(&self) -> String {
        if self.span_name.is_empty() {
            self.service_name.clone()
        } else {
            format!("{}:{}", self.service_name, self.span_name)
        }
    }
}

#[tracing::instrument(level = tracing::Level::DEBUG)]
pub async fn get_spans() -> Result<(HashMap<SpanId, Rc<RefCell<Span>>>, HashMap<TraceId, Trace>)> {
    let client = Client::default()
        .with_url("http://localhost:8080/clickhouse/")
        .with_database("otel")
        .with_user("otel")
        .with_password("otel");

    let spans_query = r#"
        WITH trace_ids AS (
            SELECT TraceId
            FROM otel.otel_traces
            WHERE ParentSpanId = ''
              AND Timestamp <= NOW() - INTERVAL '5' MINUTE
              --- AND Timestamp >= NOW() - INTERVAL '45' MINUTE
              AND ServiceName NOT IN ('frontend-web', 'frontend-proxy')
            LIMIT 10000
        )
        SELECT TraceId,
               SpanId,
               ParentSpanId,
               Timestamp as StartTimestamp,
               addNanoseconds(Timestamp, Duration) as EndTimestamp,
               Duration,
               SpanName,
               SpanKind,
               ServiceName
        FROM otel.otel_traces
        JOIN trace_ids ON otel.otel_traces.TraceId = trace_ids.TraceId
    "#;

    tracing::debug!(spans_query);

    let mut spans = client
        .with_option("wait_end_of_query", "1")
        .query(spans_query)
        .fetch_all::<ClickhouseSpan>()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .map(|spans| {
            HashMap::from_iter(spans.into_iter().map(|s| {
                (
                    s.span_id.clone(),
                    Rc::new(RefCell::new(Span {
                        arrows: vec![
                            SpanArrow::Begin {
                                time: s.start_timestamp,
                            },
                            SpanArrow::Terminate {
                                time: s.end_timestamp,
                            },
                        ],
                        inner: s,
                        children: Vec::default(),
                    })),
                )
            }))
        })?;

    let mut incomplete_traces = Vec::default();
    let mut traces: HashMap<TraceId, Trace> = HashMap::new();
    for (_, rc_span) in spans.iter() {
        let ch_span = &rc_span.borrow().inner;
        if ch_span.parent_span_id.is_empty() {
            traces.insert(
                ch_span.trace_id.clone(),
                Trace {
                    root: Rc::clone(rc_span),
                    trace_id: ch_span.trace_id.clone(),
                    duration: 0,
                },
            );
        } else {
            match spans.get(&ch_span.parent_span_id) {
                Some(parent_span) => {
                    let mut parent_span = parent_span.borrow_mut();
                    parent_span.children.push(Rc::clone(rc_span));
                    parent_span.arrows.push(SpanArrow::Forward {
                        time: ch_span.start_timestamp,
                        parent: ch_span.parent_span_id.clone(),
                        child: ch_span.span_id.clone(),
                    });
                    parent_span.arrows.push(SpanArrow::Receive {
                        time: ch_span.end_timestamp,
                        parent: ch_span.parent_span_id.clone(),
                        child: ch_span.span_id.clone(),
                    });
                    parent_span.arrows.sort();
                }
                None => {
                    incomplete_traces.push(ch_span.trace_id.clone());
                }
            }
        }
    }

    for trace_id in &incomplete_traces {
        traces.remove(trace_id);
        spans.retain(|_, v| &v.borrow().inner.trace_id != trace_id);
    }
    if !incomplete_traces.is_empty() {
        tracing::warn!(
            n_incomplete = incomplete_traces.len(),
            "Removed incomplete traces from data"
        );
    }

    let mut to_process = VecDeque::new();
    for (_, trace) in traces.iter_mut() {
        let mut earliest_start = i64::MAX;
        let mut latest_end = i64::MIN;

        to_process.clear();
        to_process.push_back(Rc::clone(&trace.root));
        while let Some(span) = to_process.pop_front() {
            let span = span.borrow();

            earliest_start = earliest_start.min(span.inner.start_timestamp);
            latest_end = latest_end.max(span.inner.end_timestamp);

            for sc in span.children.iter() {
                to_process.push_back(Rc::clone(sc));
            }
        }

        trace.duration = latest_end - earliest_start;
    }

    Ok((spans, traces))
}
