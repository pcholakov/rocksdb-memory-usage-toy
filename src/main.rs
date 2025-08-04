use anyhow::Context;
use bytesize::ByteSize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use sysinfo::Pid;
use sysinfo::System;

pub fn main() -> anyhow::Result<()> {
    const TEST_DURATION_SECONDS: u64 = 20;
    const COLUMN_FAMILY_COUNT: usize = 24; // every CF gets a writer
    const COLUMN_FAMILY_READERS: usize = 24; // capped to CF count
    const UNEVEN_WRITE_LOAD: bool = true;
    const WRITE_BUFFER_SIZE_BYTES: usize = ByteSize::mib(1000).as_u64() as usize;
    const BLOCK_CACHE_SIZE_BYTES: usize = ByteSize::mib(1000).as_u64() as usize; // excluding write buffers
    const REPORT_INTERVAL_MILLIS: u64 = 500;

    let path = Path::new("target/db");
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path)?;

    let block_cache =
        rocksdb::Cache::new_lru_cache(BLOCK_CACHE_SIZE_BYTES + WRITE_BUFFER_SIZE_BYTES);

    let write_buffer_manager = Arc::new(
        rocksdb::WriteBufferManager::new_write_buffer_manager_with_cache(
            BLOCK_CACHE_SIZE_BYTES,
            true,
            block_cache.clone(),
        ),
    );

    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_write_buffer_manager(&write_buffer_manager);

    let mut cf_opts = rocksdb::Options::default();
    cf_opts.set_write_buffer_size(write_buffer_manager.get_buffer_size() >> 2);
    cf_opts.set_max_write_buffer_number(2);

    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&block_cache);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_index_type(rocksdb::BlockBasedIndexType::BinarySearch);
    block_opts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
    cf_opts.set_block_based_table_factory(&block_opts);

    let column_family_names: Vec<_> = (0..COLUMN_FAMILY_COUNT).map(|i| format!("cf{i}")).collect();

    let db = Arc::new(rocksdb::DB::open_cf_descriptors(
        &db_opts,
        path,
        column_family_names
            .iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, cf_opts.clone()))
            .collect::<Vec<_>>(),
    )?);

    let bytes_written = &AtomicU64::new(0);
    let bytes_read = &AtomicU64::new(0);
    let running = &AtomicBool::new(true);

    std::thread::scope(|s| -> anyhow::Result<()> {
        let monitor = {
            let db = Arc::clone(&db);
            let column_family_names = column_family_names.clone();

            s.spawn(move || {
                let mut system = System::new_all();
                let pid = Pid::from_u32(std::process::id());

                let mut cf_sizes: Vec<Vec<u64>> = vec![Vec::new(); column_family_names.len()];

                println!(
                    "{:<12} {:>12} {:<8} {:>12} {:<8} | {:>12} {:>12} | {:>12} | {:>12} {:>12}",
                    "Cache usage",
                    "WBM usage",
                    "util",
                    "Cache ex-WBM",
                    "util",
                    "CF mem avg",
                    "CF mem max",
                    "RSS",
                    "written",
                    "read",
                );
                let mut last_report = Instant::now();
                loop {
                    let wbm_usage = write_buffer_manager.get_usage() as u64;
                    let wbm_size = write_buffer_manager.get_buffer_size() as u64;
                    let rss = system
                        .refresh_process(pid)
                        .then(|| system.process(pid).expect("pid exists").memory())
                        .expect("read process RSS succeeds");

                    for (cf_idx, name) in column_family_names.iter().enumerate() {
                        let cf_handle = db.cf_handle(name).expect("cf exists");
                        let cf_usage = db
                            .property_int_value_cf(&cf_handle, "rocksdb.size-all-mem-tables")
                            .expect("property exists")
                            .unwrap();
                        cf_sizes[cf_idx].push(cf_usage);
                    }

                    let elapsed = last_report.elapsed();
                    if elapsed >= Duration::from_millis(REPORT_INTERVAL_MILLIS) {
                        let block_cache_usage = block_cache.get_usage() as u64;

                        let mut cf_maxs = Vec::new();
                        let mut cf_totals = Vec::new();

                        for cf_data in &cf_sizes {
                            let max = cf_data.iter().fold(0, |a, &b| a.max(b));
                            let total = cf_data.iter().sum::<u64>() as f64;
                            cf_maxs.push(max);
                            cf_totals.push(total);
                        }
                        let max = cf_maxs.iter().fold(0, |a, &b| a.max(b));
                        let avg = cf_totals.iter().sum::<f64>()
                            / (cf_totals.len() * column_family_names.len()) as f64
                            / cf_sizes.len() as f64;

                        println!(
                            "{:<12} {:>12} {:>8} {:>12} {:>8} | {:>12} {:>12} | {:>12} | {:>12} {:>12}",
                            format!("{}", ByteSize::b(block_cache_usage).display()),
                            format!("{}", ByteSize::b(wbm_usage).display()),
                            format!("{:.1}%", (wbm_usage as f64 / wbm_size as f64) * 100.0),
                            format!(
                                "{}",
                                ByteSize::b(block_cache_usage.saturating_sub(wbm_size)).display()
                            ),
                            format!("{:.1}%", (block_cache_usage.saturating_sub(wbm_size) as f64 / BLOCK_CACHE_SIZE_BYTES as f64) * 100.0),
                            format!("{}", ByteSize::b(avg as u64).display()),
                            format!("{}", ByteSize::b(max).display()),
                            format!("{}", ByteSize::b(rss).display()),
                            format!("{}/s", ByteSize::b((bytes_written.swap(0, Ordering::Relaxed) as f64 / elapsed.as_secs_f64()) as u64).display()),
                            format!("{}/s", ByteSize::b((bytes_read.swap(0, Ordering::Relaxed) as f64 / elapsed.as_secs_f64()) as u64).display()),
                        );

                        for cf_data in &mut cf_sizes {
                            cf_data.clear();
                        }
                        last_report = Instant::now();
                    }

                    std::thread::sleep(Duration::from_millis(2));
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                }
            })
        };

        let writers: Vec<_> = column_family_names
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                let db = Arc::clone(&db);

                s.spawn(move || -> anyhow::Result<()> {
                    let cf = db.cf_handle(name).context("valid")?;
                    let mut key = 0u64;
                    let val = vec![0u8; 1024];

                    let thread_writes = match (UNEVEN_WRITE_LOAD, idx % 4) {
                        (false, _) => 1u64,
                        (_, 0) => 8,
                        (_, 1) => 4,
                        (_, 2) => 2,
                        (_, _) => 1, // 100%
                    };

                    while running.load(Ordering::Relaxed) {
                        let mut batch = rocksdb::WriteBatch::default();
                        for _ in 0..10 {
                            if key % thread_writes == 0 {
                                batch.put_cf(&cf, format!("key{}", key), &val);
                            }
                            key += 1;
                        }
                        bytes_written.fetch_add(batch.size_in_bytes() as u64, Ordering::Relaxed);
                        db.write(batch)?;
                        if key % 1000 == 0 {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    }

                    Ok(())
                })
            })
            .collect();

        let readers: Vec<_> = column_family_names
            .iter()
            .take(COLUMN_FAMILY_READERS)
            .map(|name| {
                let db = Arc::clone(&db);

                s.spawn(move || -> anyhow::Result<()> {
                    let cf = db.cf_handle(name).context("valid")?;
                    while running.load(Ordering::Relaxed) {
                        let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                        let mut count = 0;
                        for item in iter {
                            let (key, value) = item?;
                            bytes_read
                                .fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
                            count += 1;
                            if count % 10_000 == 0 && !running.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }

                    Ok(())
                })
            })
            .collect();

        std::thread::sleep(Duration::from_secs(TEST_DURATION_SECONDS));
        running.store(false, Ordering::Relaxed);

        for t in writers.into_iter().chain(readers.into_iter()) {
            t.join().unwrap()?;
        }
        monitor.join().unwrap();

        Ok(())
    })?;

    Ok(())
}
