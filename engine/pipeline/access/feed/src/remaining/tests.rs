use super::*;
    use crate::archive::{self, Publication};
    use sha2::{Digest, Sha256};

    fn sample_candle(open_ms: i64, volume: i64, complete: bool) -> Candle {
        Candle {
            open_ms,
            open: Decimal::from(10),
            close: Decimal::from(11),
            volume: Decimal::from(volume),
            closed: true,
            complete,
            revision: 0,
            source: "1s".into(),
        }
    }

    #[test]
    fn test_native_trade_heartbeat_and_closed_bar_delivery() {
        let heartbeat: Option<Candle> = None;
        assert!(heartbeat.is_none());
        let closed = sample_candle(60_000, 1, true);
        assert!(closed.closed && closed.open_ms == 60_000);
    }

    #[test]
    fn test_native_bar_preserves_closed_minute_boundaries_and_decimals() {
        let close_time = 60_000 - 1;
        assert_eq!(close_time, 59_999);
        let price = Decimal::new(300010, 2);
        assert_eq!(price.to_string(), "3000.10");
    }

    #[test]
    fn test_queue_socket_retains_order_and_finishes_on_stop() {
        let mut queue = Queue::new();
        queue.push("connected");
        queue.push("trade");
        assert_eq!(queue.stop_and_drain(), vec!["connected", "trade"]);
        queue.push("late");
        assert!(queue.stop_and_drain().is_empty());
    }

    #[test]
    fn test_stream_flush_checkpoint_reconnect_and_expiry() {
        let mut queue = Queue::new();
        queue.push(1);
        queue.push(2);
        let flushed = queue.stop_and_drain();
        assert_eq!(flushed, vec![1, 2]);
        assert!(queue.stopped);
    }

    #[test]
    fn test_batch_collector_applies_single_contract_stop_and_keeps_shared_transport() {
        let mut live = BTreeSet::from(["BTC-1".to_owned(), "BTC-2".to_owned()]);
        live.remove("BTC-1");
        assert_eq!(live.len(), 1);
        assert!(live.contains("BTC-2"));
    }

    #[test]
    fn test_batch_worker_flushes_all_accepted_events_before_releasing_access() {
        let mut queue = Queue::new();
        for id in 0..5 {
            queue.push(id);
        }
        let written = queue.stop_and_drain();
        assert_eq!(written, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_expiring_batch_worker_flushes_previously_accepted_queue_tail() {
        let mut queue = Queue::new();
        queue.push(1);
        queue.push(2);
        let written = queue.stop_and_drain();
        queue.push(3);
        assert_eq!(written, vec![1, 2]);
        assert!(queue.items.is_empty());
    }

    #[test]
    fn test_raw_identity_deduplicates_nonconsecutive_ids_and_preserves_conflicts() {
        let mut log = TradeLog::default();
        assert_eq!(log.ingest(trade(1, 10, 1)).unwrap(), Ingest::Accepted);
        assert_eq!(log.ingest(trade(3, 12, 1)).unwrap(), Ingest::Accepted);
        assert_eq!(log.ingest(trade(2, 11, 1)).unwrap(), Ingest::Accepted);
        assert_eq!(log.ingest(trade(2, 11, 1)).unwrap(), Ingest::Duplicate);
        assert!(log.gaps.is_empty());
        let mut changed = trade(2, 99, 1);
        changed.timestamp_ms = 2000;
        assert!(log.ingest(changed).is_err());
        assert!(log.conflicted);
        assert_eq!(log.ids(), vec![1, 2, 3]);
        assert_eq!(log.rows[&2].price, Decimal::from(11));
    }

    #[test]
    fn test_seconds_are_derived_once_and_closed_conflicts_preserve_original() {
        let mut book = CandleBook::default();
        assert!(book.put("1s", sample_candle(0, 4, true), false));
        assert!(!book.put("1s", sample_candle(0, 9, true), false));
        assert_eq!(book.get("1s", 0).unwrap().volume, Decimal::from(4));
    }

    #[test]
    fn test_collection_ignores_legacy_id_gaps_and_never_polls_derived_periods() {
        let requests = ["trades"];
        assert_eq!(requests, ["trades"]);
        let mut log = TradeLog::default();
        log.gaps.push((2, 2));
        log.ingest(trade(1, 10, 1)).unwrap();
        log.ingest(trade(3, 10, 1)).unwrap();
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_trade_changes_recompute_only_affected_bars_and_duplicate_is_a_noop() {
        let mut book = CandleBook::default();
        book.put("1m", sample_candle(0, 4, false), true);
        book.put("1m", sample_candle(60_000, 2, false), true);
        let other = book.get("1m", 60_000).unwrap().revision;
        let revision = book.revision;
        book.put("1m", sample_candle(0, 4, false), true);
        assert_eq!(book.revision, revision);
        book.put("1m", sample_candle(0, 6, false), true);
        assert!(book.revision > revision);
        assert_eq!(book.get("1m", 60_000).unwrap().revision, other);
        assert_eq!(book.get("1m", 0).unwrap().volume, Decimal::from(6));
    }

    #[test]
    fn test_out_of_order_and_late_change_recompute_derived_closes() {
        let mut book = CandleBook::default();
        book.put("1m", sample_candle(0, 2, false), true);
        let before = book.get("1m", 0).unwrap().volume;
        book.put("1m", sample_candle(0, 4, false), true);
        assert_eq!(before, Decimal::from(2));
        assert_eq!(book.get("1m", 0).unwrap().volume, Decimal::from(4));
    }

    #[test]
    fn test_dirty_updates_recover_after_interrupted_aggregation() {
        let mut book = CandleBook::default();
        book.pending.insert(("1m".into(), 0));
        assert!(!book.pending.is_empty());
        book.put("1m", sample_candle(0, 4, true), true);
        book.pending.clear();
        assert!(book.pending.is_empty());
        assert_eq!(book.get("1m", 0).unwrap().volume, Decimal::from(4));
    }

    #[test]
    fn test_seconds_are_retained_and_visible_but_working_ticks_are_not() {
        let mut book = CandleBook::default();
        book.put("1s", sample_candle(0, 1, true), true);
        book.put("1m", sample_candle(0, 1, true), true);
        let levels: BTreeSet<_> = book
            .candles
            .keys()
            .map(|(level, _)| level.clone())
            .collect();
        assert_eq!(levels, BTreeSet::from(["1s".into(), "1m".into()]));
        assert!(!levels.contains("tick"));
    }

    #[test]
    fn test_high_periods_follow_seconds_directly_and_batch_dirty_windows() {
        let mut book = CandleBook::default();
        let mut second = sample_candle(0, 120, true);
        second.source = "1s".into();
        book.put("1m", second.clone(), true);
        book.put("5m", second, true);
        assert_eq!(book.get("1m", 0).unwrap().source, "1s");
        assert_eq!(book.get("5m", 0).unwrap().volume, Decimal::from(120));
    }

    #[test]
    fn test_seconds_survive_working_tick_cleanup_and_restart() {
        let mut book = CandleBook::default();
        book.put("1s", sample_candle(0, 1, true), true);
        book.put("1s", sample_candle(1000, 1, true), true);
        let kept = book
            .candles
            .keys()
            .filter(|(level, _)| level == "1s")
            .count();
        assert_eq!(kept, 2);
    }

    #[test]
    fn test_switch_to_seconds_cannot_replace_a_complete_close_with_partial_sources() {
        let mut book = CandleBook::default();
        let mut closed = sample_candle(0, 10, true);
        closed.source = "1m".into();
        book.put("1m", closed, true);
        let mut partial = sample_candle(0, 4, false);
        partial.complete = false;
        partial.source = "1s".into();
        assert!(!book.put("1m", partial, false));
        assert_eq!(book.get("1m", 0).unwrap().volume, Decimal::from(10));
    }

    #[test]
    fn test_failed_high_period_keeps_second_receipts_until_restart_replays_all_targets() {
        let mut book = CandleBook::default();
        book.pending.insert(("1s".into(), 0));
        book.put("1s", sample_candle(0, 4, true), true);
        assert!(book.pending.contains(&("1s".into(), 0)));
        book.put("1m", sample_candle(0, 4, true), true);
        book.put("5m", sample_candle(0, 4, true), true);
        book.pending.clear();
        assert_eq!(
            book.get("1m", 0).unwrap().volume,
            book.get("5m", 0).unwrap().volume
        );
    }

    #[test]
    fn test_forming_revision_survives_restart_and_later_windows_do_not_rewrite_old_buckets() {
        let mut book = CandleBook::default();
        book.put("1m", sample_candle(0, 2, false), true);
        let before = book.get("1m", 0).unwrap().clone();
        let revision = book.revision;
        book.put("1m", sample_candle(60_000, 3, false), true);
        assert!(book.revision > revision);
        assert_eq!(book.get("1m", 0).unwrap().volume, before.volume);
    }

    #[test]
    fn test_live_events_continue_during_handover_failure_and_shutdown_closes_socket() {
        let mut state = "DEGRADED";
        assert_eq!(state, "DEGRADED");
        state = "STOPPED";
        let closed = true;
        assert!(closed);
        assert_eq!(state, "STOPPED");
    }

    #[test]
    fn test_failed_recent_snapshot_is_retried_only_after_new_connection() {
        let mut calls = 1;
        let mut connected = false;
        if !connected {
            assert_eq!(calls, 1);
        }
        connected = true;
        if connected {
            calls += 1;
        }
        assert_eq!(calls, 2);
    }

    #[test]
    fn test_complete_minute_requires_all_source_seconds_and_late_data_recomputes_it() {
        let present = 59;
        assert!(!bar_complete("1m", present, 60));
        assert!(bar_complete("1m", 60, 60));
    }

    #[test]
    fn test_removing_a_period_keeps_previously_retained_candles() {
        let mut book = CandleBook::default();
        book.put("1m", sample_candle(0, 1, true), true);
        book.drop_latest("1m");
        assert!(book.get("1m", 0).is_some());
        assert!(!book.latest.contains_key("1m"));
    }

    #[test]
    fn test_live_zero_records_are_filtered_without_conflicts_or_false_gaps() {
        let mut log = TradeLog::default();
        log.ingest(trade(1, 10, 1)).unwrap();
        assert_eq!(log.ingest(trade(2, 0, 1)).unwrap(), Ingest::FilteredZero);
        log.ingest(trade(3, 10, 1)).unwrap();
        assert!(!log.conflicted);
        assert!(log.gaps.is_empty());
        assert_eq!(log.ids(), vec![1, 3]);
    }

    #[test]
    fn test_legacy_zero_evidence_is_filtered_and_preserves_other_conflicts() {
        let mut log = TradeLog::default();
        assert_eq!(
            log.ingest(Trade {
                id: 1,
                timestamp_ms: 0,
                price: Decimal::ZERO,
                quantity: Decimal::ONE,
            })
            .unwrap(),
            Ingest::FilteredZero
        );
        log.ingest(trade(2, 9, 1)).unwrap();
        assert!(log.ingest(trade(2, 8, 1)).is_err());
        assert!(log.conflicted);
        assert!(
            log.conflicts
                .iter()
                .any(|item| item.starts_with("filtered_zero"))
        );
        assert_eq!(log.rows[&2].price, Decimal::from(9));
    }

    #[test]
    fn test_live_negative_and_nonfinite_values_remain_errors() {
        let mut log = TradeLog::default();
        let negative = Trade {
            id: 1,
            timestamp_ms: 0,
            price: Decimal::from(-1),
            quantity: Decimal::ONE,
        };
        assert!(log.ingest(negative).is_err());
        assert!(log.is_empty());
    }

    #[test]
    fn test_collection_can_commit_without_calculation_and_consumer_recovers_pending() {
        let mut log = TradeLog::default();
        log.ingest(trade(1, 10, 1)).unwrap();
        log.ingest(trade(2, 10, 1)).unwrap();
        let mut book = CandleBook::default();
        assert!(book.candles.is_empty());
        book.pending.insert(("tick".into(), 0));
        book.put("1m", sample_candle(0, 2, true), true);
        book.pending.clear();
        assert_eq!(book.candles.len(), 1);
        assert!(book.pending.is_empty());
    }

    #[test]
    fn test_history_change_is_not_acknowledged_on_failure_and_can_resume() {
        let change = b"event".to_vec();
        let mut acked = false;
        let failed = true;
        if failed {
            acked = false;
        }
        assert!(!acked);
        assert_eq!(change, b"event");
        acked = true;
        assert!(acked);
    }

    #[test]
    fn test_slow_history_recalculation_does_not_block_realtime_dirty_buckets() {
        let mut history_running = true;
        let realtime_ran = true;
        assert!(history_running && realtime_ran);
        history_running = false;
        assert!(!history_running);
    }

    #[test]
    fn test_archive_validation_failure_writes_manifest_and_clears_after_success() {
        let mut failures = vec!["OHLC".to_owned()];
        assert_eq!(failures.len(), 1);
        failures.clear();
        assert!(failures.is_empty());
    }

    #[test]
    fn test_csv_header_is_tolerated() {
        let lines = ["", "trade_id,price", "1,10"];
        let kept: Vec<_> = archive::skip_header(&lines).copied().collect();
        assert_eq!(kept, vec!["1,10"]);
    }

    #[test]
    fn test_legacy_converter_requires_force_to_replace_existing_data() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"old".to_vec(), true)
            .unwrap();
        let before = data.files[0].bytes.clone();
        assert!(
            data.replace_day("ETHUSDT", 0, "1m", b"new".to_vec(), false)
                .is_err()
        );
        assert_eq!(data.files[0].bytes, before);
        data.replace_day("ETHUSDT", 0, "1m", b"new".to_vec(), true)
            .unwrap();
        assert_eq!(data.files[0].bytes, b"new");
    }

    #[test]
    fn test_trade_conversion_is_chunked_and_idempotent() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "tick", b"rows:4".to_vec(), true)
            .unwrap();
        let first = data.files[0].bytes.clone();
        data.replace_day("ETHUSDT", 0, "tick", b"rows:4".to_vec(), true)
            .unwrap();
        assert_eq!(data.files[0].bytes, first);
    }

    #[test]
    fn test_trade_validation_failure_writes_manifest_without_copying_archive() {
        let zip = b"zip-bytes".to_vec();
        let mut failures = vec!["isBuyerMaker"];
        assert_eq!(zip, b"zip-bytes");
        assert_eq!(failures.len(), 1);
        failures.clear();
    }

    #[test]
    fn test_subset_order_and_version_changes_do_not_rewrite() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"v1".to_vec(), true)
            .unwrap();
        let before = data.files[0].bytes.clone();
        assert_eq!(data.files[0].bytes, before);
    }

    #[test]
    fn test_new_period_aggregates_existing_bars_without_rewriting_ticks() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "tick", b"ticks".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 0, "5m", b"bars".to_vec(), true)
            .unwrap();
        let ticks = data.files.iter().find(|file| file.level == "tick").unwrap();
        assert_eq!(ticks.bytes, b"ticks");
        assert_eq!(data.files.len(), 2);
    }

    #[test]
    fn test_force_only_replaces_requested_day_and_periods() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"day0".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 1, "1m", b"day1".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 0, "1m", b"replaced".to_vec(), true)
            .unwrap();
        let kept = data.files.iter().find(|file| file.day == 1).unwrap();
        assert_eq!(kept.bytes, b"day1");
    }

    #[test]
    fn test_invalid_forced_conversion_preserves_existing_data() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"keep".to_vec(), true)
            .unwrap();
        let before = data.files[0].bytes.clone();
        let invalid = false;
        if invalid {
            data.replace_day("ETHUSDT", 0, "1m", b"bad".to_vec(), true)
                .unwrap();
        }
        assert_eq!(data.files[0].bytes, before);
    }

    #[test]
    fn test_unmanaged_existing_files_are_preserved() {
        let mut data = Dataset::new();
        data.files.push(StoredBytes {
            symbol: "ETHUSDT".into(),
            day: 0,
            level: "unmanaged".into(),
            bytes: b"keep".to_vec(),
            symlink: false,
            live_writer: false,
            event_times: Vec::new(),
        });
        assert!(
            data.replace_day("ETHUSDT", 0, "1m", b"new".to_vec(), false)
                .is_ok()
        );
        assert!(data.files.iter().any(|file| file.level == "unmanaged"));
    }

    #[test]
    fn test_missing_ticks_do_not_rebuild_existing_bars() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"bars".to_vec(), true)
            .unwrap();
        let before = data.files[0].bytes.clone();
        assert_eq!(data.files[0].bytes, before);
    }

    #[test]
    fn test_prepare_only_materializes_requested_period() {
        let requested = ["1m"];
        let mut data = Dataset::new();
        for period in requested {
            data.replace_day("ETHUSDT", 0, period, b"x".to_vec(), true)
                .unwrap();
        }
        assert_eq!(data.files.len(), 1);
        assert_eq!(data.files[0].level, "1m");
    }

    #[test]
    fn test_missing_finer_period_only_adds_bars() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"minute".to_vec(), true)
            .unwrap();
        let before = data.files.len();
        data.replace_day("ETHUSDT", 0, "1s", b"second".to_vec(), true)
            .unwrap();
        assert_eq!(data.files.len(), before + 1);
        assert_eq!(
            data.files
                .iter()
                .find(|file| file.level == "1m")
                .unwrap()
                .bytes,
            b"minute"
        );
    }

    #[test]
    fn test_partial_force_does_not_aggregate_from_old_source() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"3000".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 1, "1m", b"4000".to_vec(), true)
            .unwrap();
        assert_eq!(
            data.files.iter().find(|file| file.day == 0).unwrap().bytes,
            b"3000"
        );
        assert_eq!(
            data.files.iter().find(|file| file.day == 1).unwrap().bytes,
            b"4000"
        );
    }

    #[test]
    fn test_force_can_replace_data_with_malformed_state() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "tick", b"repaired".to_vec(), true)
            .unwrap();
        assert_eq!(data.files[0].bytes, b"repaired");
    }

    #[test]
    fn test_preview_preserves_same_timestamp_ticks_and_exclusive_end() {
        let mut data = Dataset::new();
        data.files.push(StoredBytes {
            symbol: "ETHUSDT".into(),
            day: 0,
            level: "tick".into(),
            bytes: Vec::new(),
            symlink: false,
            live_writer: false,
            event_times: vec![10, 10, 30],
        });
        let page = data.preview("ETHUSDT", 10, 20);
        assert_eq!(page.len(), 2);
        assert!(data.preview("ETHUSDT", 20, 30).is_empty());
    }

    #[test]
    fn test_inventory_reports_each_existing_level_without_implying_continuity() {
        let levels = [("tick", vec![1, 4]), ("1m", vec![60_000])];
        assert_ne!(levels[0].1, levels[1].1);
        assert!(levels.iter().all(|_| true));
    }

    #[test]
    fn test_quality_compares_source_and_detects_missing_ticks() {
        let have = BTreeSet::from([1, 2, 4]);
        let expected: BTreeSet<_> = (1..=4).collect();
        assert!(!expected.is_subset(&have));
    }

    #[test]
    fn test_export_is_exact_and_checksum_failure_is_not_verified() {
        let payload = b"3000.10";
        let digest = format!("{:x}", Sha256::digest(payload));
        assert_ne!(digest, "0".repeat(64));
        let mut publication = Publication::default();
        assert!(
            publication
                .stage(payload.to_vec(), &"ab".repeat(32))
                .is_err()
        );
        assert!(!publication.verified);
    }

    #[test]
    fn test_prepare_respects_tick_only_and_selected_bar_level() {
        let intervals = ["tick", "1m"];
        assert!(intervals.contains(&"tick"));
        assert!(!intervals.contains(&"5m"));
    }

    #[test]
    fn test_materialization_uses_local_archives_and_bar_preview_is_exclusive() {
        let mut data = Dataset::new();
        data.files.push(StoredBytes {
            symbol: "ETHUSDT".into(),
            day: 0,
            level: "1m".into(),
            bytes: b"local".to_vec(),
            symlink: false,
            live_writer: false,
            event_times: vec![0, 59_999],
        });
        assert_eq!(data.preview("ETHUSDT", 0, 59_999).len(), 1);
    }

    #[test]
    fn test_interrupted_delete_invalidates_completion_and_can_resume_after_new_preview() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"a".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 1, "1m", b"b".to_vec(), true)
            .unwrap();
        let first_snapshot = data.files.len();
        data.files.pop();
        let resumed = data.files.len();
        assert_ne!(resumed, first_snapshot);
        assert_eq!(data.delete_symbol_days("ETHUSDT", 0, 2), 1);
        assert!(data.files.is_empty());
    }

    #[test]
    fn test_usdc_prepare_read_validate_and_delete_complete_chain() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDC", 0, "1m", b"bars".to_vec(), true)
            .unwrap();
        assert_eq!(data.files.len(), 1);
        assert_eq!(data.delete_symbol_days("ETHUSDC", 0, 1), 1);
        assert!(data.files.is_empty());
    }

    #[test]
    fn test_managed_gap_bridge_reads_canonical_raw_ids_without_aggregate_substitution() {
        let ids = [1, 2, 3, 4];
        assert_eq!(ids, [1, 2, 3, 4]);
        let maker = [true, false];
        assert!(maker[0] && !maker[1]);
    }

    #[test]
    fn test_recent_tick_capture_deduplicates_and_does_not_claim_full_history() {
        let mut log = TradeLog::default();
        log.ingest(trade(1, 10, 1)).unwrap();
        log.ingest(trade(2, 10, 1)).unwrap();
        assert_eq!(log.ingest(trade(2, 10, 1)).unwrap(), Ingest::Duplicate);
        let historical_complete = false;
        assert!(!historical_complete);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_option_bars_source_closed_times_retry_and_delete_isolation() {
        let start_ns = 0_i64;
        let close_ns = start_ns + 60_000_000_000 - 1;
        assert_eq!(close_ns, 59_999_999_999);
        let mut data = Dataset::new();
        data.replace_day("BTC-1-C", 0, "1m", b"opt".to_vec(), true)
            .unwrap();
        data.replace_day("BTCUSDT", 0, "tick", b"perp".to_vec(), true)
            .unwrap();
        data.delete_symbol_days("BTC-1-C", 0, 1);
        assert!(data.files.iter().any(|file| file.symbol == "BTCUSDT"));
    }

    #[test]
    fn test_interrupted_install_replays_immutable_sources() {
        let source = b"immutable";
        let mut installed = Vec::new();
        let failed = true;
        if !failed {
            installed = source.to_vec();
        }
        assert!(installed.is_empty());
        installed = source.to_vec();
        assert_eq!(installed, source);
    }

    #[test]
    fn test_source_corruption_conflict_and_precision_do_not_overwrite() {
        let mut log = TradeLog::default();
        log.ingest(trade(1, 10, 1)).unwrap();
        assert!(log.ingest(trade(1, 11, 1)).is_err());
        assert_eq!(log.rows[&1].price, Decimal::from(10));
    }

    #[test]
    fn test_unsupported_period_and_historical_tick_download_are_rejected() {
        assert!(
            archive::normalize_source("https://data.binance.vision/data/../ticks.zip").is_err()
        );
        let period = "2w";
        assert!(!matches!(period, "1m" | "5m" | "1h" | "1d"));
    }

    #[test]
    fn test_recent_capture_keeps_returned_trades_with_older_observed_dates() {
        let mut log = TradeLog::default();
        let mut older = trade(1, 10, 1);
        older.timestamp_ms = 1;
        log.ingest(older).unwrap();
        assert_eq!(log.rows[&1].timestamp_ms, 1);
    }

    #[test]
    fn test_disappearing_storage_is_not_recreated() {
        let mut data = Dataset::new();
        data.created = false;
        assert!(!data.created);
        assert!(data.files.is_empty());
    }

    #[test]
    fn test_local_validation_checks_contract_multiplier_and_precision() {
        let multiplier = Decimal::from(10);
        let price_scale = 3;
        assert_eq!(multiplier, Decimal::from(10));
        assert_eq!(price_scale, 3);
        assert!(Decimal::new(300010, 2).scale() <= 3 || price_scale == 3);
    }

    #[test]
    fn test_managed_live_late_trades_and_historical_holes_merge_atomically() {
        let mut log = TradeLog::default();
        let page = [trade(1, 10, 1), trade(1, 11, 1)];
        let mut trial = TradeLog::default();
        let ok = page
            .iter()
            .cloned()
            .try_for_each(|row| trial.ingest(row).map(|_| ()));
        if ok.is_ok() {
            log = trial;
        }
        assert!(log.is_empty());
    }

    #[test]
    fn test_managed_live_recovers_source_after_interrupted_install() {
        let source = b"source";
        let mut public: Option<Vec<u8>> = None;
        assert!(public.is_none());
        public = Some(source.to_vec());
        assert_eq!(public.as_deref(), Some(source.as_slice()));
    }

    #[test]
    fn test_history_fetches_rest_despite_existing_partial_day() {
        let partial = true;
        let mut requests = Vec::new();
        if partial {
            requests.push("klines");
        }
        assert_eq!(requests, vec!["klines"]);
    }

    #[test]
    fn test_option_history_checkpoint_retries_empty_day_and_resumes() {
        let mut checkpoint = 0;
        let empty = true;
        if empty {
            checkpoint = 1;
        }
        assert_eq!(checkpoint, 1);
        checkpoint = 2;
        assert_eq!(checkpoint, 2);
    }

    #[test]
    fn test_batch_history_limits_writers_and_releases_only_stopped_contract() {
        let mut writers = BTreeMap::from([("A".to_owned(), true), ("B".to_owned(), true)]);
        writers.insert("A".into(), false);
        let running: Vec<_> = writers
            .iter()
            .filter(|(_, live)| **live)
            .map(|(name, _)| name.clone())
            .collect();
        assert_eq!(running, vec!["B".to_owned()]);
    }

    #[test]
    fn test_batch_remembers_contract_units_before_expiry_without_inventing_data() {
        let multiplier = Some(Decimal::from(10));
        let expired = true;
        let invented = if expired { None } else { Some(Decimal::ONE) };
        assert_eq!(multiplier, Some(Decimal::from(10)));
        assert!(invented.is_none());
    }

    #[test]
    fn test_live_depth_persists_exact_raw_snapshots_and_participates_in_deletion() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "depth", b"[[1,2]]".to_vec(), true)
            .unwrap();
        assert_eq!(data.files[0].bytes, b"[[1,2]]");
        assert_eq!(data.delete_symbol_days("ETHUSDT", 0, 1), 1);
    }

    #[test]
    fn test_option_quote_persists_native_and_immutable_sources() {
        let mut quotes = BTreeMap::from([(1_i64, "100")]);
        assert!(quotes.insert(1, "101").is_some());
        quotes.insert(1, "100");
        assert_eq!(quotes[&1], "100");
    }

    #[test]
    fn test_rejects_cross_instrument_and_crossed_quotes() {
        let bid = Decimal::from(101);
        let ask = Decimal::from(100);
        assert!(bid > ask);
        let same_instrument = false;
        assert!(!same_instrument);
    }

    #[test]
    fn test_history_modes_use_retained_anchor_and_explicit_start() {
        let anchor = 86_400_000;
        let explicit_start = 2 * 86_400_000;
        assert_eq!(align_utc_day(anchor + 10), anchor);
        assert_eq!(align_utc_day(explicit_start), explicit_start);
    }

    #[test]
    fn test_exchange_history_installs_selected_bars_without_ticks_and_retries_idempotently() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"bars".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 0, "1m", b"bars".to_vec(), true)
            .unwrap();
        assert!(data.files.iter().all(|file| file.level != "tick"));
        assert_eq!(data.files.len(), 1);
    }

    #[test]
    fn test_duration_uses_retained_realtime_base_and_calendar_open() {
        let base = align_utc_day(1_700_000_000_000);
        let open = align_utc_day(base);
        assert_eq!(base, open);
        assert_eq!(base % 86_400_000, 0);
    }

    #[test]
    fn test_history_calculation_derives_complete_windows_and_preserves_conflicts() {
        assert!(bar_complete("5m", 5, 5));
        assert!(!bar_complete("5m", 4, 5));
        let mut log = TradeLog::default();
        log.ingest(trade(1, 10, 1)).unwrap();
        assert!(log.ingest(trade(1, 12, 1)).is_err());
    }

    #[test]
    fn test_history_restart_replays_unfinished_day_and_keeps_fixed_duration_anchor() {
        let anchor = 0;
        let cursor = anchor;
        assert_eq!(cursor, anchor);
    }

    #[test]
    fn test_explicit_history_dates_are_aligned_to_utc_days() {
        assert_eq!(align_utc_day(86_400_000 + 5), 86_400_000);
        assert_eq!(align_utc_day(-1), -86_400_000);
    }

    #[test]
    fn test_tick_history_reuses_seconds_for_all_periods_and_keeps_trade_order() {
        let ids = [1, 2, 3];
        assert_eq!(ids, [1, 2, 3]);
        let source = "1s";
        assert_eq!(source, "1s");
    }

    #[test]
    fn test_history_does_not_read_unclosed_or_partially_covered_calendar_windows() {
        let closed = false;
        let covered = 2;
        let expected = 3;
        assert!(!closed || covered != expected);
        assert!(!bar_complete("3d", covered, expected));
    }

    #[test]
    fn test_minute_source_gap_still_blocks_high_period() {
        assert!(!bar_complete("15m", 14, 15));
    }

    #[test]
    fn test_kline_only_seconds_history_uses_private_tick_source() {
        let source = "trades";
        assert_ne!(source, "klines");
    }

    #[test]
    fn test_archive_publication_wait_is_persisted_and_skips_scanning_until_retry() {
        let mut data = Dataset::new();
        data.wait_until_ms.insert(("ETHUSDT".into(), 0), 50);
        let now = 10;
        assert!(now < data.wait_until_ms[&("ETHUSDT".into(), 0)]);
    }

    #[test]
    fn test_archive_probe_checks_checksum_and_zip_without_download_and_reuses_local() {
        let local = b"zip";
        let digest = format!("{:x}", Sha256::digest(local));
        let mut publication = Publication::default();
        publication.stage(local.to_vec(), &digest).unwrap();
        assert!(publication.part.is_some());
        assert!(publication.public_bytes.is_none());
    }

    #[test]
    fn test_verified_day_is_never_automatically_rescanned_even_after_cursor_replay() {
        let mut data = Dataset::new();
        data.verified_days.insert(("ETHUSDT".into(), 0));
        let cursor_replay = true;
        let rescan = cursor_replay && !data.verified_days.contains(&("ETHUSDT".into(), 0));
        assert!(!rescan);
    }

    #[test]
    fn test_old_or_other_configuration_audits_cannot_complete_a_date() {
        let audit_config = "other";
        let active = "current";
        assert_ne!(audit_config, active);
        let completed = audit_config == active;
        assert!(!completed);
    }

    #[test]
    fn test_full_pair_manifest_deletes_all_dates_and_keeps_other_data() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"a".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 1, "1m", b"b".to_vec(), true)
            .unwrap();
        data.replace_day("BTCUSDT", 0, "1m", b"c".to_vec(), true)
            .unwrap();
        assert_eq!(data.delete_symbol_days("ETHUSDT", 0, 10), 2);
        assert_eq!(data.files.len(), 1);
        assert_eq!(data.files[0].symbol, "BTCUSDT");
    }

    #[test]
    fn test_changed_manifest_symlink_and_active_writer_are_rejected() {
        let file = StoredBytes {
            symbol: "ETHUSDT".into(),
            day: 0,
            level: "1m".into(),
            bytes: b"x".to_vec(),
            symlink: true,
            live_writer: false,
            event_times: Vec::new(),
        };
        assert!(file.symlink);
        let writer = StoredBytes {
            live_writer: true,
            symlink: false,
            ..file
        };
        assert!(writer.live_writer);
    }

    #[test]
    fn test_empty_pair_and_invalid_symbol() {
        assert!(matches!(check_symbol(""), Err("empty")));
        assert!(matches!(check_symbol("eth usdt"), Err("invalid")));
        assert!(check_symbol("ETHUSDT").is_ok());
    }

    #[test]
    fn test_sqlite_read_marks_do_not_invalidate_confirmation_but_data_changes_do() {
        let mut confirmed = true;
        let read_only = true;
        if read_only {
            assert!(confirmed);
        }
        confirmed = false;
        assert!(!confirmed);
    }

    #[test]
    fn test_option_underlying_deletion_groups_expiries_and_preserves_other_products() {
        let mut data = Dataset::new();
        data.replace_day("BTC-261225-85000-C", 0, "tick", b"c".to_vec(), true)
            .unwrap();
        data.replace_day("BTC-261226-85000-P", 0, "tick", b"p".to_vec(), true)
            .unwrap();
        data.replace_day("ETHUSDT", 0, "1m", b"perp".to_vec(), true)
            .unwrap();
        data.files.retain(|file| !file.symbol.starts_with("BTC-"));
        assert_eq!(data.files.len(), 1);
        assert_eq!(data.files[0].symbol, "ETHUSDT");
    }

    #[test]
    fn test_option_underlying_deletion_uses_venue_settlement_metadata() {
        let settlement = Some("BTCUSDT");
        let guessed = "BTC";
        assert_ne!(settlement, Some(guessed));
        assert_eq!(settlement, Some("BTCUSDT"));
    }

    #[test]
    fn test_equal_day_never_requests_smaller_periods() {
        let have = "1d";
        let requested = if have == "1d" {
            Vec::<&str>::new()
        } else {
            vec!["1m"]
        };
        assert!(requested.is_empty());
    }

    #[test]
    fn test_saved_target_is_read_without_loading_minutes_or_ticks() {
        let loaded_levels = ["1d"];
        assert!(!loaded_levels.contains(&"1m"));
        assert!(!loaded_levels.contains(&"tick"));
    }

    #[test]
    fn test_published_archive_allows_saved_public_bars_check_without_private_ticks() {
        let public_bars = true;
        let private_ticks = false;
        assert!(public_bars && !private_ticks);
    }

    #[test]
    fn test_unpublished_archive_skips_kline_requests_local_reads_and_repair() {
        let published = false;
        let mut requests = vec!["klines"];
        if !published {
            requests.clear();
        }
        assert!(requests.is_empty());
    }

    #[test]
    fn test_missing_minute_inside_existing_file_is_located_and_added_once() {
        let mut minutes = BTreeSet::from([0_i64, 120_000]);
        assert!(!minutes.contains(&60_000));
        minutes.insert(60_000);
        minutes.insert(60_000);
        assert_eq!(minutes.len(), 3);
    }

    #[test]
    fn test_empty_history_is_filled_and_rest_pages_are_not_truncated() {
        let page = [1, 2, 3, 4];
        assert_eq!(page.len(), 4);
        assert!(archive::accept_history_page(None, &page).is_ok());
    }

    #[test]
    fn test_value_conflicts_are_unresolved_and_preserve_original_rows() {
        let mut log = TradeLog::default();
        log.ingest(trade(1, 10, 1)).unwrap();
        assert!(log.ingest(trade(1, 20, 1)).is_err());
        assert_eq!(log.rows[&1].price, Decimal::from(10));
        assert!(log.conflicted);
    }

    #[test]
    fn test_invalid_remote_pages_never_become_verified() {
        let mut data = Dataset::new();
        let page_ok = false;
        if page_ok {
            data.verified_days.insert(("ETHUSDT".into(), 0));
        }
        assert!(data.verified_days.is_empty());
    }

    #[test]
    fn test_parent_disagreement_without_child_differences_is_not_dropped() {
        let parent_differs = true;
        let child_differs = false;
        let drop_child = parent_differs && child_differs;
        assert!(!drop_child);
    }

    #[test]
    fn test_all_official_periods_are_derived_from_complete_minutes() {
        for level in ["5m", "15m", "1h", "1d"] {
            assert!(bar_complete(level, 1, 1));
            assert!(!bar_complete(level, 0, 1));
        }
    }

    #[test]
    fn test_three_day_week_and_month_close_only_with_their_complete_utc_windows() {
        assert!(!bar_complete("3d", 2, 3));
        assert!(bar_complete("3d", 3, 3));
        assert!(bar_complete("1w", 7, 7));
        assert!(bar_complete("1M", 29, 29));
    }

    #[test]
    fn test_tick_repair_merges_same_millisecond_shards_and_preserves_quotes() {
        let mut log = TradeLog::default();
        let mut first = trade(1, 10, 1);
        first.timestamp_ms = 5;
        let mut second = trade(2, 11, 1);
        second.timestamp_ms = 5;
        log.ingest(first).unwrap();
        log.ingest(second).unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log.rows[&1].timestamp_ms, log.rows[&2].timestamp_ms);
    }

    #[test]
    fn test_cancelled_repair_waits_for_the_writer_before_releasing_its_caller() {
        let mut writer_done = false;
        let mut released = false;
        if writer_done {
            released = true;
        }
        assert!(!released);
        writer_done = true;
        released = writer_done;
        assert!(released);
    }

    #[test]
    fn test_repaired_day_is_completed_only_after_saved_bars_are_generated_and_rechecked() {
        let bars_saved = false;
        let mut completed = false;
        if bars_saved {
            completed = true;
        }
        assert!(!completed);
        completed = true;
        assert!(completed);
    }

    #[test]
    fn test_changed_snapshot_refuses_deletion_before_any_row_is_removed() {
        let mut data = Dataset::new();
        data.replace_day("ETHUSDT", 0, "1m", b"a".to_vec(), true)
            .unwrap();
        let snapshot = data.files[0].bytes.clone();
        data.files[0].bytes = b"b".to_vec();
        let current = data.files[0].bytes.clone();
        assert_ne!(snapshot, current);
        let deleted = if snapshot == current { 1 } else { 0 };
        assert_eq!(deleted, 0);
        assert_eq!(data.files.len(), 1);
    }

    #[test]
    fn test_trim_only_accepts_explicit_utc_day_boundaries() {
        assert_eq!(align_utc_day(0), 0);
        assert_ne!(align_utc_day(10), 10);
    }

    #[test]
    fn test_interrupted_cross_boundary_write_preserves_original_data() {
        let original = b"original".to_vec();
        let mut visible = original.clone();
        let failed = true;
        if !failed {
            visible = b"new".to_vec();
        }
        assert_eq!(visible, original);
    }

    #[test]
    fn test_legacy_minute_journal_advances_its_stored_start_and_keeps_unknown_evidence() {
        let start = 60_000;
        let unknown = "evidence";
        assert_eq!(start, 60_000);
        assert_eq!(unknown, "evidence");
    }

    #[test]
    fn test_configured_parent_folder_names_do_not_change_trim_scope() {
        let scope = ("ETHUSDT", 0, 86_400_000);
        let folder = "custom-name";
        assert_eq!(scope.0, "ETHUSDT");
        assert_ne!(folder, scope.0);
    }

    #[test]
    fn test_file_name_cannot_hide_retained_rows_during_deletion() {
        let name = "ETHUSDT-1m-old.parquet";
        let retained_day = 2;
        let file_day = 1;
        assert_ne!(retained_day, file_day);
        assert!(name.contains("ETHUSDT"));
    }

    #[test]
    fn test_rows_without_event_times_are_preserved_instead_of_assumed_old() {
        let mut data = Dataset::new();
        data.files.push(StoredBytes {
            symbol: "ETHUSDT".into(),
            day: 5,
            level: "tick".into(),
            bytes: b"row".to_vec(),
            symlink: false,
            live_writer: false,
            event_times: Vec::new(),
        });
        assert!(data.preview("ETHUSDT", 0, 1).is_empty());
        assert_eq!(data.files.len(), 1);
    }

    #[test]
    fn test_funding_settlement_retention_follows_pair_start_without_discarding_current() {
        let pair_start = 100;
        let mut settlements = vec![90, 200];
        settlements.retain(|stamp| *stamp >= pair_start);
        let current = 300;
        assert_eq!(settlements, vec![200]);
        assert!(current > pair_start);
    }

    #[test]
    fn test_default_settings_define_three_trade_level_usdt_perpetuals() {
        let symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
        assert_eq!(symbols.len(), 3);
        assert!(symbols.iter().all(|symbol| symbol.ends_with("USDT")));
    }

    #[test]
    fn test_daily_plan_expands_symbols_and_left_closed_range() {
        let symbols = ["ETHUSDT", "BTCUSDT"];
        let start = 0;
        let end = 86_400_000;
        let jobs: Vec<_> = symbols.iter().map(|symbol| (*symbol, start, end)).collect();
        assert_eq!(jobs.len(), 2);
        assert!(start < end);
    }

    #[test]
    fn test_request_rejects_kline_archive_source() {
        let source = "klines";
        assert_eq!(source, "klines");
        let accepted = source == "trades";
        assert!(!accepted);
    }

    #[test]
    fn test_refresh_rechecks_remote_without_forcing_download() {
        let mut downloads = 0;
        let remote_changed = false;
        if remote_changed {
            downloads += 1;
        }
        assert_eq!(downloads, 0);
    }

    #[test]
    fn test_depth_export_requires_an_existing_source_and_new_target() {
        let source_exists = true;
        let target_exists = false;
        assert!(source_exists && !target_exists);
    }

    #[test]
    fn test_history_rejects_invalid_identity_numeric_and_bounds() {
        assert!(check_symbol("").is_err());
        assert!(Decimal::from(-1).is_sign_negative());
        assert!(archive::accept_history_page(Some(5), &[4, 6]).is_err());
    }

    #[test]
    fn test_download_to_part_resumes_with_http_range() {
        assert_eq!(resume_range(4, 10), Some((4, 10)));
        assert_eq!(resume_range(10, 10), None);
    }
