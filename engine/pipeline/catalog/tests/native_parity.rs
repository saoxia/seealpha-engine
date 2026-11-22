//! Explicit offline Python/Nautilus v2 RC9 -> Rust -> Python interoperability
//! gate. Prepare the fixtures with tests/interop.py before running this test.
//! The default Rust test suite never requires Python or touches an old volume.
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use seealpha_catalog::{Catalog, RecordKind, bar_type};
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Deserialize)]
struct Fixture {
    kind: String,
    identifier: String,
    start_ns: i64,
    end_ns: i64,
    symbol: Option<String>,
    product: Option<String>,
    interval: Option<String>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    price_precision: Option<u32>,
    size_precision: Option<u32>,
}

#[test]
#[ignore = "requires an isolated Python NautilusTrader RC9 fixture directory"]
fn python_native_to_rust_roundtrip() {
    let root = std::env::var("SEEALPHA_CATALOG_PARITY_ROOT")
        .expect("set SEEALPHA_CATALOG_PARITY_ROOT to the offline fixture root");
    let root = Path::new(&root);
    let source = Catalog::open_existing(&root.join("python")).unwrap();
    let target = root.join("rust");
    fs::create_dir_all(&target).unwrap();
    let rust = Catalog::open_existing(&target).unwrap();
    let fixtures: Vec<Fixture> =
        serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert!(!fixtures.is_empty());
    for item in fixtures {
        let kind = RecordKind::from_name(&item.kind).unwrap();
        if item.kind == "bars" {
            let symbol = item.symbol.as_deref().unwrap();
            let product = item.product.as_deref().unwrap();
            let interval = item.interval.as_deref().unwrap();
            // Fixture adapter only; production mapping lives in Source.
            let instrument = match product {
                "spot" | "options" => format!("{symbol}.BINANCE"),
                "futures_um" => format!("{symbol}-PERP.BINANCE"),
                _ => panic!("unknown fixture product"),
            };
            let identity = bar_type(&instrument, interval).unwrap();
            let original = source
                .read_bars(
                    &identity,
                    interval,
                    item.start_ns,
                    item.end_ns,
                    50_000,
                )
                .unwrap();
            assert!(!original.is_empty());
            let path = rust
                .append_bars(
                    &identity,
                    interval,
                    &original,
                    item.price_precision.unwrap(),
                    item.size_precision.unwrap(),
                )
                .unwrap();
            assert!(path.is_file());
            assert_eq!(
                rust.read_bars(
                    &identity,
                    interval,
                    item.start_ns,
                    item.end_ns,
                    50_000
                )
                .unwrap(),
                original,
            );
            assert!(
                source
                    .bar_gaps(
                        &identity,
                        interval,
                        item.start_ms.unwrap(),
                        item.end_ms.unwrap(),
                        50_000,
                    )
                    .unwrap()
                    .is_empty()
            );
        } else {
            let expected = source
                .read_records(&kind, &item.identifier, item.start_ns, item.end_ns, 50_000)
                .unwrap();
            assert!(
                !expected.is_empty(),
                "missing Python fixture: {:?}",
                item.kind
            );
            let path = source
                .root()
                .join(kind.directory(&item.identifier).unwrap());
            let files = fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                .collect::<Vec<_>>();
            assert_eq!(
                files.len(),
                1,
                "fixtures must contain one file per data type"
            );
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&files[0]).unwrap())
                    .unwrap()
                    .with_batch_size(2048)
                    .build()
                    .unwrap();
            let batches = reader.map(|result| result.unwrap()).collect::<Vec<_>>();
            let published = rust
                .append_record_batches(&kind, &item.identifier, &batches)
                .unwrap();
            assert!(published.is_file());
            assert_eq!(
                rust.read_records(&kind, &item.identifier, item.start_ns, item.end_ns, 50_000,)
                    .unwrap(),
                expected,
            );
            assert_eq!(
                rust.append_record_batches(&kind, &item.identifier, &batches)
                    .unwrap(),
                published,
            );
        }
    }
}
