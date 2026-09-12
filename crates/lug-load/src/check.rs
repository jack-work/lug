use crate::{Result, error};
use lug_proto::{Durability, Record};
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

pub fn patch(sequence: u64, size: usize) -> serde_json::Value {
    serde_json::Value::String(format!("{sequence:016x}{}", "x".repeat(size - 16)))
}

struct Issued {
    log: usize,
    intended: Instant,
    ack: Option<u64>,
    observed: Option<u64>,
}

struct Stream {
    log: usize,
    next: u64,
    credit: u64,
}

pub struct Ledger {
    issued: Vec<Issued>,
    canonical: Vec<BTreeMap<u64, u64>>,
    streams: HashMap<u64, Stream>,
    patch_size: usize,
    durability: Durability,
    ack_count: usize,
}

impl Ledger {
    pub fn new(logs: usize, patch_size: usize, durability: Durability) -> Self {
        Self {
            issued: Vec::new(),
            canonical: vec![BTreeMap::new(); logs],
            streams: HashMap::new(),
            patch_size,
            durability,
            ack_count: 0,
        }
    }

    pub fn issue(&mut self, log: usize, intended: Instant) -> u64 {
        let sequence = self.issued.len() as u64;
        self.issued.push(Issued {
            log,
            intended,
            ack: None,
            observed: None,
        });
        sequence
    }

    pub fn subscribe(&mut self, id: u64, log: usize, from: u64, credit: u32) -> Result<()> {
        if self
            .streams
            .insert(
                id,
                Stream {
                    log,
                    next: from + 1,
                    credit: u64::from(credit),
                },
            )
            .is_some()
        {
            return Err(error(format!("duplicate harness stream id {id}")));
        }
        Ok(())
    }

    pub fn grant(&mut self, id: u64, credit: u32) -> Result<()> {
        let stream = self
            .streams
            .get_mut(&id)
            .ok_or_else(|| error(format!("credit for unknown stream {id}")))?;
        stream.credit += u64::from(credit);
        Ok(())
    }

    fn bind(&mut self, log: usize, version: u64, sequence: u64) -> Result<()> {
        if version == 0 {
            return Err(error(format!("log {log}: observed version zero")));
        }
        if let Some(old) = self.canonical[log].insert(version, sequence) {
            if old != sequence {
                return Err(error(format!(
                    "ORDER MISMATCH: log {log} version {version}: payload {old} vs {sequence}"
                )));
            }
        }
        let issued = self.issued.get_mut(sequence as usize).ok_or_else(|| {
            error(format!(
                "log {log} version {version}: unissued payload {sequence}"
            ))
        })?;
        if issued.log != log {
            return Err(error(format!(
                "payload {sequence} belongs to log {}, received on log {log}",
                issued.log
            )));
        }
        if let Some(old) = issued.observed {
            if old != version {
                return Err(error(format!(
                    "DUPLICATE APPEND: payload {sequence} mapped to versions {old} and {version}"
                )));
            }
        }
        issued.observed = Some(version);
        Ok(())
    }

    pub fn ack(&mut self, sequence: u64, versions: &[u64], synced: u64) -> Result<Instant> {
        if versions.len() != 1 {
            return Err(error(format!(
                "append {sequence}: one Noop patch must mint one version, got {versions:?}"
            )));
        }
        let issued = self
            .issued
            .get(sequence as usize)
            .ok_or_else(|| error(format!("ack of unissued append {sequence}")))?;
        if issued.ack.is_some() {
            return Err(error(format!("DUPLICATE ACK: append {sequence}")));
        }
        if self.durability == Durability::Durable && synced < versions[0] {
            return Err(error(format!(
                "durable append {sequence}: synced {synced} below acknowledged version {}",
                versions[0]
            )));
        }
        self.bind(issued.log, versions[0], sequence)?;
        let issued = &mut self.issued[sequence as usize];
        issued.ack = Some(versions[0]);
        self.ack_count += 1;
        Ok(issued.intended)
    }

    pub fn records(&mut self, id: u64, records: &[Record]) -> Result<Vec<Instant>> {
        let mut intended = Vec::with_capacity(records.len());
        for record in records {
            let stream = self
                .streams
                .get_mut(&id)
                .ok_or_else(|| error(format!("records on unknown stream {id}")))?;
            if record.version != stream.next {
                return Err(error(format!(
                    "NONCONTIGUOUS STREAM {id}: expected version {}, got {} (gap, duplicate or reorder)",
                    stream.next, record.version
                )));
            }
            if stream.credit == 0 {
                return Err(error(format!(
                    "CREDIT VIOLATION: stream {id} pushed version {} without credit",
                    record.version
                )));
            }
            stream.credit -= 1;
            stream.next += 1;
            let log = stream.log;
            let text = record.patch.as_str().ok_or_else(|| {
                error(format!(
                    "log {log} version {}: payload is not a string",
                    record.version
                ))
            })?;
            if text.len() != self.patch_size
                || !text.is_ascii()
                || !text[16..].bytes().all(|b| b == b'x')
            {
                return Err(error(format!(
                    "log {log} version {}: payload bytes changed",
                    record.version
                )));
            }
            let sequence = u64::from_str_radix(&text[..16], 16).map_err(|_| {
                error(format!(
                    "log {log} version {}: invalid payload sequence",
                    record.version
                ))
            })?;
            self.bind(log, record.version, sequence)?;
            intended.push(self.issued[sequence as usize].intended);
        }
        Ok(intended)
    }

    pub fn complete(&self) -> bool {
        self.ack_count == self.issued.len()
            && self.streams.values().all(|s| {
                s.next - 1
                    == self.canonical[s.log]
                        .last_key_value()
                        .map_or(0, |(v, _)| *v)
            })
    }

    pub fn finish(&self) -> Result<()> {
        if self.ack_count != self.issued.len() {
            return Err(error(format!(
                "ACK LOSS: issued {}, acknowledged {}",
                self.issued.len(),
                self.ack_count
            )));
        }
        for (id, stream) in &self.streams {
            let end = self.canonical[stream.log]
                .last_key_value()
                .map_or(0, |(v, _)| *v);
            if stream.next - 1 != end {
                return Err(error(format!(
                    "SUBSCRIBER LOSS: stream {id} observed {}, acknowledged through {end}",
                    stream.next - 1
                )));
            }
        }
        for (log, versions) in self.canonical.iter().enumerate() {
            if versions.last_key_value().map_or(0, |(v, _)| *v) != versions.len() as u64 {
                return Err(error(format!(
                    "log {log}: acknowledged versions are not contiguous from 1"
                )));
            }
        }
        Ok(())
    }

    pub fn replay(&mut self) {
        self.streams.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Ledger {
        let mut l = Ledger::new(1, 32, Durability::Durable);
        l.issue(0, Instant::now());
        l.issue(0, Instant::now());
        l.subscribe(10, 0, 0, 2).unwrap();
        l.subscribe(11, 0, 0, 2).unwrap();
        l
    }
    fn record(version: u64, sequence: u64) -> Record {
        Record {
            version,
            patch: patch(sequence, 32),
        }
    }

    #[test]
    fn records_can_race_ack_and_subscribers_must_agree() {
        let mut l = fixture();
        l.records(10, &[record(1, 1), record(2, 0)]).unwrap();
        l.ack(0, &[2], 2).unwrap();
        l.ack(1, &[1], 2).unwrap();
        assert!(!l.complete());
        l.records(11, &[record(1, 1), record(2, 0)]).unwrap();
        l.finish().unwrap();
        l.replay();
        l.subscribe(12, 0, 0, 2).unwrap();
        l.records(12, &[record(1, 1), record(2, 0)]).unwrap();
        l.finish().unwrap();
    }

    #[test]
    fn reject_gap_duplicate_wrong_order_and_lost_ack() {
        assert!(fixture().records(10, &[record(2, 0)]).is_err());
        assert!(
            fixture()
                .records(10, &[record(1, 0), record(1, 0)])
                .is_err()
        );
        let mut l = fixture();
        l.records(10, &[record(1, 0)]).unwrap();
        assert!(l.records(11, &[record(1, 1)]).is_err());
        assert!(fixture().finish().is_err());
    }

    #[test]
    fn reject_duplicate_payload_durability_lie_and_credit_overrun() {
        let mut l = fixture();
        l.ack(0, &[1], 1).unwrap();
        assert!(l.ack(0, &[1], 1).is_err());
        assert!(l.records(10, &[record(1, 0), record(2, 0)]).is_err());
        assert!(fixture().ack(0, &[1], 0).is_err());
        let mut l = fixture();
        l.subscribe(20, 0, 0, 0).unwrap();
        assert!(l.records(20, &[record(1, 0)]).is_err());
    }

    #[test]
    fn ack_does_not_excuse_missing_delivery_or_changed_payload() {
        let mut l = fixture();
        l.ack(0, &[1], 1).unwrap();
        l.ack(1, &[2], 2).unwrap();
        l.records(10, &[record(1, 0), record(2, 1)]).unwrap();
        l.records(11, &[record(1, 0)]).unwrap();
        assert!(l.finish().is_err());
        let corrupt = Record {
            version: 2,
            patch: serde_json::json!("0000000000000001xxxxxxxxxxxxxxxy"),
        };
        assert!(l.records(11, &[corrupt]).is_err());
    }
}
