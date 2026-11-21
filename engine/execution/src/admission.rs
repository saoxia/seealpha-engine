//! Venue-independent durable execution admission and one-shot send policy.
//!
//! Transport credentials, exchange order routes, and protocol ACK parsing
//! remain with the execution client. The Ledger controls admission and
//! persists outcomes; a duplicate request MUST NOT produce another write.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    Fresh,
    Seen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordedOutcome {
    Accepted,
    Rejected,
    Unknown,
}

pub trait IntentJournal<Record, Error> {
    /// Commit the immutable intent before returning Fresh.
    /// A crash, replay, or existing request identity must not re-grant Fresh.
    fn admit(&mut self, record: &Record) -> Result<Admission, Error>;

    /// An acknowledgement is not a trade execution; persist only the
    /// outcome of attempting the already-admitted command.
    fn observe(&mut self, record: &Record, outcome: RecordedOutcome) -> Result<(), Error>;
}

/// Execute at most one external side effect per PreparedWrite instance and
/// exactly zero on duplicated or failed journal admission.
///
/// Durable, cross-restart idempotency is owned by Journal::admit. A sender's
/// error is never proof that an order was not accepted. Invalid ACKs and failed
/// result persistence remain unconfirmed and may only be reconciled by reads.
pub fn dispatch_once<Record, Journal, Error, Reply, Send, Verify, Unknown>(
    attempted: &mut bool,
    record: &Record,
    journal: &mut Journal,
    send: Send,
    verify_ack: Verify,
    unconfirmed: Unknown,
) -> Result<Reply, Error>
where
    Journal: IntentJournal<Record, Error>,
    Send: FnOnce() -> Result<Reply, Error>,
    Verify: FnOnce(&Reply) -> bool,
    Unknown: Fn() -> Error,
{
    if *attempted {
        return Err(unconfirmed());
    }
    *attempted = true;

    if journal.admit(record)? != Admission::Fresh {
        return Err(unconfirmed());
    }

    let result = send().map_err(|_| unconfirmed()).and_then(|reply| {
        if verify_ack(&reply) {
            Ok(reply)
        } else {
            Err(unconfirmed())
        }
    });
    let outcome = if result.is_ok() {
        RecordedOutcome::Accepted
    } else {
        RecordedOutcome::Unknown
    };
    journal
        .observe(record, outcome)
        .map_err(|_| unconfirmed())?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Problem {
        Unknown,
    }

    #[derive(Default)]
    struct Journal {
        ids: BTreeSet<String>,
        outcomes: Vec<RecordedOutcome>,
        fail_admission: bool,
        fail_observation: bool,
    }

    impl IntentJournal<String, Problem> for Journal {
        fn admit(&mut self, record: &String) -> Result<Admission, Problem> {
            if self.fail_admission {
                return Err(Problem::Unknown);
            }
            Ok(if self.ids.insert(record.clone()) {
                Admission::Fresh
            } else {
                Admission::Seen
            })
        }

        fn observe(&mut self, _: &String, outcome: RecordedOutcome) -> Result<(), Problem> {
            if self.fail_observation {
                return Err(Problem::Unknown);
            }
            self.outcomes.push(outcome);
            Ok(())
        }
    }

    #[test]
    fn duplicate_and_reused_instance_never_send_again() {
        let mut journal = Journal::default();
        let mut attempted = false;
        let request = "same-request".to_owned();
        let mut sends = 0;
        let first = dispatch_once(
            &mut attempted,
            &request,
            &mut journal,
            || { sends += 1; Ok::<_, Problem>("ACK") },
            |ack| *ack == "ACK",
            || Problem::Unknown,
        );
        assert_eq!(first, Ok("ACK"));
        assert_eq!(sends, 1);
        assert_eq!(journal.outcomes, vec![RecordedOutcome::Accepted]);
        assert_eq!(
            dispatch_once(
                &mut attempted, &request, &mut journal,
                || { sends += 1; Ok::<_, Problem>("ACK") },
                |ack| *ack == "ACK",
                || Problem::Unknown,
            ),
            Err(Problem::Unknown)
        );
        let mut another_instance = false;
        assert_eq!(
            dispatch_once(
                &mut another_instance, &request, &mut journal,
                || { sends += 1; Ok::<_, Problem>("ACK") },
                |ack| *ack == "ACK",
                || Problem::Unknown,
            ),
            Err(Problem::Unknown)
        );
        assert_eq!(sends, 1);
    }

    #[test]
    fn timeout_or_unverified_ack_stays_unknown_and_never_reposts() {
        let mut journal = Journal::default();
        let mut attempted = false;
        let request = "timed-out".to_owned();
        let mut sends = 0;
        assert_eq!(
            dispatch_once(
                &mut attempted, &request, &mut journal,
                || { sends += 1; Err::<&str, _>(Problem::Unknown) },
                |_| true,
                || Problem::Unknown,
            ),
            Err(Problem::Unknown)
        );
        assert_eq!(journal.outcomes, vec![RecordedOutcome::Unknown]);
        assert_eq!(sends, 1);

        let mut other = false;
        assert_eq!(
            dispatch_once(
                &mut other, &"bad-ack".to_owned(), &mut journal,
                || { sends += 1; Ok::<_, Problem>("unverified") },
                |ack| *ack == "ACK",
                || Problem::Unknown,
            ),
            Err(Problem::Unknown)
        );
        assert_eq!(journal.outcomes, vec![RecordedOutcome::Unknown, RecordedOutcome::Unknown]);
        assert_eq!(sends, 2);
    }

    #[test]
    fn admission_or_observation_failure_fails_closed() {
        let request = "request".to_owned();
        let mut journal = Journal { fail_admission: true, ..Journal::default() };
        let mut attempted = false;
        let mut sends = 0;
        assert_eq!(
            dispatch_once(
                &mut attempted, &request, &mut journal,
                || { sends += 1; Ok::<_, Problem>("ACK") },
                |_| true,
                || Problem::Unknown,
            ),
            Err(Problem::Unknown)
        );
        assert_eq!(sends, 0);

        let mut journal = Journal { fail_observation: true, ..Journal::default() };
        let mut attempted = false;
        assert_eq!(
            dispatch_once(
                &mut attempted, &request, &mut journal,
                || { sends += 1; Ok::<_, Problem>("ACK") },
                |_| true,
                || Problem::Unknown,
            ),
            Err(Problem::Unknown)
        );
        assert_eq!(sends, 1);
    }
}
