//! Synthetic offline archive adversarial checks. No Python execution or exchange I/O.
use seealpha_strategy::{inspect_archive, sha256_hex};
use std::io::{Cursor, Write};
use zip::{ZipWriter, write::SimpleFileOptions};

const MANIFEST: &str = r#"{"format":1,"name":"alpha_strategy","title":"Alpha","version":"1.2","entry_point":"strategy:Strategy","parameters":{"symbol":"ETHUSDT","allocation":"1","risk_limit":"1"}}"#;

fn archive(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for &(name, content) in files {
        writer
            .start_file(name, SimpleFileOptions::default())
            .unwrap();
        writer.write_all(content).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[test]
fn legitimate_package_has_stable_digest_and_versioned_entry() {
    let bytes = archive(&[
        ("seealpha-package.json", MANIFEST.as_bytes()),
        ("strategy.py", b"class Strategy: pass"),
    ]);
    let meta = inspect_archive(&bytes).unwrap();
    assert_eq!(meta.name, "alpha_strategy");
    assert_eq!(meta.version, "1.2");
    assert_eq!(meta.entry_point, "strategy:Strategy");
    assert_eq!(meta.digest, sha256_hex(&bytes));
}

#[test]
fn refuses_missing_entry_and_duplicate_names() {
    let bytes = archive(&[("seealpha-package.json", MANIFEST.as_bytes())]);
    assert!(inspect_archive(&bytes).is_err());
    // Newer ZIP writers refuse duplicate names. Craft a valid-format adversarial
    // archive by renaming the equal-length local and central directory entries.
    let mut bytes = archive(&[
        ("seealpha-package.json", MANIFEST.as_bytes()),
        ("strategy.py", b"x=1"),
        ("strategy.px", b"x=2"),
    ]);
    let from = b"strategy.px";
    let to = b"strategy.py";
    let mut replaced = 0;
    for i in 0..=bytes.len() - from.len() {
        if &bytes[i..i + from.len()] == from {
            bytes[i..i + to.len()].copy_from_slice(to);
            replaced += 1;
        }
    }
    assert_eq!(replaced, 2);
    assert!(inspect_archive(&bytes).is_err());
}

#[test]
fn refuses_path_traversal_and_removed_python_import() {
    for name in [
        "../strategy.py",
        "/tmp/strategy.py",
        "nested/../../strategy.py",
    ] {
        let bytes = archive(&[
            ("seealpha-package.json", MANIFEST.as_bytes()),
            (name, b"x=1"),
            ("strategy.py", b"class Strategy: pass"),
        ]);
        assert!(inspect_archive(&bytes).is_err(), "accepted {name}");
    }
    let bytes = archive(&[
        ("seealpha-package.json", MANIFEST.as_bytes()),
        ("strategy.py", b"from strategy_python import Registry"),
    ]);
    assert!(inspect_archive(&bytes).is_err());
}

#[test]
fn rejects_wrong_manifest_type_and_missing_entry_module() {
    for manifest in [
        r#"{"format":true,"name":"alpha_strategy","version":"1.2","entry_point":"strategy:Strategy"}"#,
        r#"{"format":1,"name":"alpha_strategy","version":"../bad","entry_point":"strategy:Strategy"}"#,
        r#"{"format":1,"name":"alpha_strategy","version":"1.2","entry_point":"not_found:Strategy"}"#,
    ] {
        let bytes = archive(&[
            ("seealpha-package.json", manifest.as_bytes()),
            ("strategy.py", b"class Strategy: pass"),
        ]);
        assert!(inspect_archive(&bytes).is_err(), "{manifest}");
    }
}

#[test]
fn rejects_too_large_archive_and_excessive_member_count() {
    assert!(inspect_archive(&vec![0u8; 256 * 1024 + 1]).is_err());
    let mut files = vec![
        (
            "seealpha-package.json".to_string(),
            MANIFEST.as_bytes().to_vec(),
        ),
        ("strategy.py".to_string(), b"x=1".to_vec()),
    ];
    for i in 0..32 {
        files.push((format!("extra{i}.txt"), b"x".to_vec()));
    }
    let borrowed: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, data)| (name.as_str(), data.as_slice()))
        .collect();
    assert!(inspect_archive(&archive(&borrowed)).is_err());
}
