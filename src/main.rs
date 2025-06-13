//! By changing config.json, we see that when a process consistently bursts above its requested CPU, it can starve other processes
//! CPU requests manifest in the form of cpu.weight inside the cgroupsv2 cpu controller
//! It doesn't guarantee exclusive access to a core
//! 

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::process::Stdio;
use std::time::Instant;
use std::collections::HashMap;

const SHARES_PER_CPU: u64 = 1024;
const TEST_SLICE: &str = "test.slice";
const TRIALS: u8 = 10;

#[derive(Clone, Copy)]
pub struct BenchmarkResult {
    id: u64,
    duration_millis: u64,
    cpu_usage_millis: u64
}

// Analyze benchmark results through several statistics
fn analyze_benchmarks(trial_results: Vec<Vec<BenchmarkResult>>){
    let num_trials: u64 = trial_results.len() as u64; 
    let mut id_to_results: HashMap<u64, BenchmarkResult> = HashMap::new();
    for trial in trial_results {
        for result in trial {
            let entry = id_to_results.entry(result.id).or_insert_with(|| BenchmarkResult { id: result.id, duration_millis: 0, cpu_usage_millis: 0 });
            entry.cpu_usage_millis += result.cpu_usage_millis;
            entry.duration_millis += result.duration_millis;
        }
    }
    for (_,v) in id_to_results.iter_mut() {
        v.cpu_usage_millis = v.cpu_usage_millis / num_trials;
        v.duration_millis = v.duration_millis / num_trials;
    }

    for (k,v) in id_to_results.iter() {
        println!("Process {}: duration_millis={}, cpu_usage_millis={}", k, v.duration_millis, v.cpu_usage_millis)
    }
}

pub fn cpu_shares_to_weight(shares: u64) -> u64 {
    // https://github.com/google/gvisor/blob/676b9db40f430143d01123d124c3806e61fce7fa/runsc/cgroup/cgroup_v2.go#L742
    1 + ((shares - 2) * 9999) / 262142
}

#[derive(Clone, Serialize, Deserialize)]
struct ProcessConfig {
    id: u64,
    threads: u64,
    cpu_request: u64,
    cpu_affinity: Option<Vec<u8>>,
    command: Option<Vec<String>>
}

#[derive(Serialize, Deserialize)]
struct Config {
    outer_iters: u64,
    inner_iters: u64,
    processes: Vec<ProcessConfig>
}

fn parse_config_file(config_file_name: &str) -> Result<Config> {
    let config_file_path = std::path::Path::new(config_file_name);
    let config_content = fs::read_to_string(config_file_path)?;
    let config: Config = serde_json::from_str(&config_content)?;
    Ok(config)
}

struct ProcessExecutor {
    pub outer_iters: u64,
    pub inner_iters: u64,
    pub processes: Vec<ProcessConfig>
}

impl ProcessExecutor {
    pub fn from_config(config: Config) -> Self {
        Self { outer_iters:config.outer_iters, inner_iters: config.inner_iters, processes: config.processes }
    }

    pub async fn execute_all(&self) -> Result<Vec<BenchmarkResult>> {
        let mut ret: Vec<BenchmarkResult> = Vec::new();
        let mut tasks = Vec::new();
        for process_config in self.processes.iter() {
            let config = process_config.clone();
            let outer_iters = self.outer_iters;
            let inner_iters = self.inner_iters;
            println!("Spawning {}",config.id);
            tasks.push((tokio::spawn(async move {
                let result = Self::execute_process(config, outer_iters, inner_iters).await;
                result
            }),process_config.id));
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }

        for (task, id) in tasks {
            match task.await? {
                Ok(result) => ret.push(result),
                Err(e) => panic!("Process {} failed with error: {}",id,e)
            }
        }

        Ok(ret)
    }

    async fn configure_cgroup(cmd: &mut tokio::process::Command, config: &ProcessConfig) -> Result<()> {
        let cpu_weight = cpu_shares_to_weight(SHARES_PER_CPU * config.cpu_request);
        let cpuset_cpus = match &config.cpu_affinity {
            Some(cpus) => cpus.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","),
            None => "".to_string()
        };
        
        // Give transient scope cpu,cpuset,memory cgroup controllers
        cmd
            .arg("--property").arg("Delegate=cpu cpuset io memory pids")
            .arg("--property").arg(format!("CPUWeight={}",cpu_weight))
            .arg("--quiet");

        // cpuset
        if cpuset_cpus.len()>0 {
            cmd.arg("--property").arg(format!("AllowedCPUs={}", cpuset_cpus));
        }

        Ok(())
    }

    async fn execute_process(config: ProcessConfig, outer_iters: u64, inner_iters: u64) -> Result<BenchmarkResult> {

        let scope = format!("p{}.scope", config.id);

        let mut cmd = tokio::process::Command::new("systemd-run");
        cmd
            .arg("--scope")
            .arg("--slice")
            .arg(TEST_SLICE)
            .arg("--unit")
            .arg(&scope);
        Self::configure_cgroup(&mut cmd, &config).await?; // ProcessExecutor::configure_cgroup(&mut cmd, &config).await?; also works

        match config.command {
            Some(v) => {
                cmd.arg("--");
                for s in v {
                    cmd.arg(s);
                }
            }
            None => {
                cmd
                    .arg("--")
                    .arg(std::env::current_exe()?)
                    .arg("worker")
                    .arg(config.threads.to_string()) // # threads == # cores used by process
                    .arg(outer_iters.to_string())
                    .arg(inner_iters.to_string());
            }
        }
        cmd.stdout(Stdio::null()).stderr(Stdio::piped());

        let start_time = Instant::now();
        let cmd_output = cmd.spawn()?.wait_with_output().await?;
        let duration = start_time.elapsed();

        if !cmd_output.status.success() {
            anyhow::bail!("Child process {} failed: {}", config.id, String::from_utf8(cmd_output.stderr)?);
        }


        let cpu_stat_filename = format!("/sys/fs/cgroup/{}/{}/cpu.stat",TEST_SLICE, scope);
        let cpu_stat_path = std::path::Path::new(cpu_stat_filename.as_str());
        let cpu_stat_string = fs::read_to_string(cpu_stat_path)?;
        let mut cpu_usage_usec: u64 = 0;
        for line in cpu_stat_string.split("\n") {
            if let Some(x) = line.strip_prefix("usage_usec ") {
                cpu_usage_usec = x.parse().context("Failed to parse CPU stat file")?;
            }
        }
        let cpu_usage_millis: u64 = cpu_usage_usec / 1000;

        Ok(BenchmarkResult{
            duration_millis: duration.as_millis().try_into()?,
            id: config.id,
            cpu_usage_millis: cpu_usage_millis
        })
    }

}
 
// Some random CPU intensive task with some number of threads
fn run_worker(threads: u8, outer_iters: u64, inner_iters: u64) -> Result<()> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(threads as usize).build()?;
    pool.install(|| {
        use rayon::prelude::*;
        let _ = (0..outer_iters).into_par_iter().map(|i| {
            let mut sum: f64 = 0.0;
            for j in 0..inner_iters {
                sum += (i as f64).sqrt() * (j as f64).powf(2.0); // random calculations
                sum = sum.sqrt();
            }
            sum
        }).sum::<f64>();
    });
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len()==5 && args[1].as_str() == "worker" {
        let threads: u8 = args[2].parse()?;
        let outer_iters: u64 = args[3].parse()?;
        let inner_iters: u64 = args[4].parse()?;
        run_worker(threads, outer_iters, inner_iters)
    } else {
        let config_file_name = args[1].as_str();
        let config = parse_config_file(config_file_name)?;
        let process_executor = ProcessExecutor::from_config(config);
        let runtime = tokio::runtime::Runtime::new()?;
        
        let mut trial_results: Vec<Vec<BenchmarkResult>> = vec![];
        // run TRIALS number of trials
        for _ in 0..TRIALS {
            let ret = runtime.block_on(process_executor.execute_all())?;
            trial_results.push(ret);
        }
        analyze_benchmarks(trial_results);
        Ok(())
    }
}