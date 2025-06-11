# Process Executor for Benchmarking

Specify processes to execute, with their configurations in a `config.json` file. The config specification is:

- `id`: name of process
- `threads`: number of threads in rayon threadpool
- `cores_requested`: scales linearly with Cgroup `cpu.weight`
- `cpu_affinity`: cores to pin the process to
