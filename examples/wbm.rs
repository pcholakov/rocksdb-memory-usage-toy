use anyhow::Context;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub fn main() -> anyhow::Result<()> {
    // let path = tempdir::TempDir::new("db")?;

    let path = Path::new("target/db");
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path)?;

    let wbm = rocksdb::WriteBufferManager::new_write_buffer_manager_with_cache(
        1_000_000,
        false,
        rocksdb::Cache::new_lru_cache(500_000),
    );

    let mut db_options = rocksdb::Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);

    let mut cf_opts = rocksdb::Options::default();
    cf_opts.set_write_buffer_manager(&wbm);

    const COLUMN_FAMILY_COUNT: usize = 28;
    let column_families: Vec<_> = (1..=COLUMN_FAMILY_COUNT)
        .map(|i| rocksdb::ColumnFamilyDescriptor::new(format!("cf{i}"), cf_opts.clone()))
        .collect();

    let db = Arc::new(rocksdb::DB::open_cf_descriptors(
        &db_options,
        path,
        column_families,
    )?);

    let column_family_names: Vec<_> = (1..=COLUMN_FAMILY_COUNT)
        .map(|i| format!("cf{i}"))
        .collect();

    let keep_writing = Arc::new(AtomicBool::new(true));

    std::thread::scope(|s| -> anyhow::Result<()> {
        let handles: Vec<_> = column_family_names
            .iter()
            .map(|name| {
                let db = Arc::clone(&db);
                let keep_writing = Arc::clone(&keep_writing);

                s.spawn(move || -> anyhow::Result<()> {
                    let cf = db.cf_handle(&name).context("cf is created")?;
                    let val = [0u8; 1000];

                    let mut k = 0usize;
                    while keep_writing.load(Ordering::Relaxed) {
                        db.put_cf(&cf, format!("k{}", k), &val)?;
                        k += 1;
                    }

                    Ok(())
                })
            })
            .collect();

        std::thread::sleep(Duration::from_secs(20));
        keep_writing.store(false, Ordering::Relaxed);

        for h in handles {
            h.join().unwrap()?;
        }

        Ok(())
    })?;

    drop(db);

    // rocksdb::DB::destroy(&db_options, path).context("destroy")?;

    Ok(())
}
