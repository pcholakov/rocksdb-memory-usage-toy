use anyhow::Context;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub fn main() -> anyhow::Result<()> {
    let path = Path::new("target/db");
    let _ = std::fs::remove_dir_all(path);
    std::fs::create_dir_all(path)?;

    const COLUMN_FAMILY_COUNT: usize = 24;
    const WRITE_BUFFER_MANAGER_CAPACITY_MIB: usize = 1000;

    let wbm = Arc::new(
        rocksdb::WriteBufferManager::new_write_buffer_manager_with_cache(
            WRITE_BUFFER_MANAGER_CAPACITY_MIB * 1 << 20,
            true,
            rocksdb::Cache::new_lru_cache(0 * 1 << 20),
        ),
    );

    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_write_buffer_manager(&*wbm);

    let mut cf_opts = rocksdb::Options::default();
    cf_opts.set_write_buffer_size(wbm.get_buffer_size() >> 2);
    cf_opts.set_max_write_buffer_number(2);

    let column_family_names: Vec<_> = (0..COLUMN_FAMILY_COUNT).map(|i| format!("cf{i}")).collect();

    let db = Arc::new(rocksdb::DB::open_cf_descriptors(
        &db_opts,
        path,
        column_family_names
            .iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, cf_opts.clone()))
            .collect::<Vec<_>>(),
    )?);

    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    println!(
        "Buffer size: {:.2} MB",
        wbm.get_buffer_size() as f64 / 1_048_576.0
    );
    println!(
        "Writing to {} column families with 1KB values each",
        COLUMN_FAMILY_COUNT
    );
    println!("Will run for 20 seconds...\n");

    std::thread::scope(|s| -> anyhow::Result<()> {
        let monitor_handle = {
            let wbm = Arc::clone(&wbm);
            let db = Arc::clone(&db);
            let running = Arc::clone(&running);
            let column_family_names = column_family_names.clone();

            s.spawn(move || {
                let mut overall_max_usage = 0usize;
                let mut total_samples = 0u64;
                let mut overall_total_usage = 0u64;

                let mut last_report = Instant::now();
                let mut sample_usages = Vec::new();
                let mut cf_samples: Vec<Vec<f64>> = vec![Vec::new(); column_family_names.len()];

                while running.load(Ordering::Relaxed) {
                    let usage = wbm.get_usage();
                    let buffer_size = wbm.get_buffer_size();

                    overall_max_usage = overall_max_usage.max(usage);
                    total_samples += 1;
                    overall_total_usage += usage as u64;

                    sample_usages.push(usage);

                    for (cf_idx, name) in column_family_names.iter().enumerate() {
                        let cf_handle = db.cf_handle(name).expect("cf exists");
                        if let Ok(Some(cf_usage)) = db.property_int_value_cf(&cf_handle, "rocksdb.size-all-mem-tables") {
                            cf_samples[cf_idx].push(cf_usage as f64 / 1_048_576.0);
                        }
                    }

                    if last_report.elapsed() >= Duration::from_millis(2000) {
                        let elapsed = start_time.elapsed().as_secs_f64();

                        let current_usage = usage;
                        let usage_percent = (current_usage as f64 / buffer_size as f64) * 100.0;

                        let sample_min = sample_usages.iter().min().copied().unwrap_or(0);
                        let sample_max = sample_usages.iter().max().copied().unwrap_or(0);
                        let sample_avg = sample_usages.iter().sum::<usize>() as f64 / sample_usages.len() as f64;

                        println!(
                            "[{:6.1}s] WBM Current: {:8.2} MB / {:4.2} MB ({:5.1}%) | Sample Stats ({}): min={:.2}MB avg={:.2}MB max={:.2}MB",
                            elapsed,
                            current_usage as f64 / 1_048_576.0,
                            buffer_size as f64 / 1_048_576.0,
                            usage_percent,
                            sample_usages.len(),
                            sample_min as f64 / 1_048_576.0,
                            sample_avg / 1_048_576.0,
                            sample_max as f64 / 1_048_576.0
                        );

                        // let block_cache_usage = db.property_int_value_cf(
                        //     db.cf_handle("default").unwrap(), "rocksdb.block-cache-usage").unwrap().unwrap();
                        // println!("          block cache: {:6.2} MB", block_cache_usage as f64 / 1_048_576.0);


                        let mut cf_mins = Vec::new();
                        let mut cf_maxs = Vec::new();
                        let mut cf_avgs = Vec::new();
                        let mut total_current = 0.0;

                        for cf_data in &cf_samples {
                            if !cf_data.is_empty() {
                                let min = cf_data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                                let max = cf_data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                                let avg = cf_data.iter().sum::<f64>() / cf_data.len() as f64;
                                let current = cf_data.last().copied().unwrap_or(0.0);

                                cf_mins.push(min);
                                cf_maxs.push(max);
                                cf_avgs.push(avg);
                                total_current += current;
                            }
                        }

                        if !cf_mins.is_empty() {
                            let min_of_mins = cf_mins.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                            let max_of_maxs = cf_maxs.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                            let avg_of_avgs = cf_avgs.iter().sum::<f64>() / cf_avgs.len() as f64;

                            println!("          CF stats:      min={:.2}MB avg={:.2}MB max={:.2}MB, total={:.2}MB (across {} CFs)\n",
                                    min_of_mins, avg_of_avgs, max_of_maxs, total_current, cf_mins.len());
                        }

                        sample_usages.clear();
                        for cf_data in &mut cf_samples {
                            cf_data.clear();
                        }
                        last_report = Instant::now();
                    }

                    std::thread::sleep(Duration::from_millis(3));
                }

                println!("\nStatistics:");
                println!("   Max usage: {:.2} MB ({:.1}%)",
                    overall_max_usage as f64 / 1_048_576.0,
                    (overall_max_usage as f64 / wbm.get_buffer_size() as f64) * 100.0
                );
                println!("   Avg usage: {:.2} MB", overall_total_usage as f64 / total_samples as f64 / 1_048_576.0);
                println!("   Samples taken: {}", total_samples);
            })
        };

        let handles: Vec<_> = column_family_names
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                let db = Arc::clone(&db);
                let running = Arc::clone(&running);

                s.spawn(move || -> anyhow::Result<()> {
                    let cf = db.cf_handle(&name).context("cf is created")?;
                    let val = vec![0u8; 1000]; // 1KB values

                    let mut k = 0usize;
                    let mut ops_count = 0u64;
                    // let thread_start = Instant::now();

                    while running.load(Ordering::Relaxed) {
                        db.put_cf(&cf, format!("thread{}_key{}", idx, k), &val)?;
                        k += 1;
                        ops_count += 1;

                        if ops_count % 100 == 0 {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                    }

                    // let thread_elapsed = thread_start.elapsed().as_secs_f64();
                    // println!(
                    //     "Thread {} completed: {} ops in {:.1}s ({:.0} ops/sec)",
                    //     idx,
                    //     ops_count,
                    //     thread_elapsed,
                    //     ops_count as f64 / thread_elapsed
                    // );

                    Ok(())
                })
            })
            .collect();

        std::thread::sleep(Duration::from_secs(20));
        running.store(false, Ordering::Relaxed);

        for h in handles {
            h.join().unwrap()?;
        }

        monitor_handle.join().unwrap();

        Ok(())
    })?;

    drop(db);

    Ok(())
}
