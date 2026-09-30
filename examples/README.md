# Examples

| Path | Shows |
|---|---|
| [`redis_demo/`](redis_demo/) | Smallest runnable `ToolModule`: input/output/setup models, one trigger, `ModuleServer` |
| [`modules/dynamic_setup_module.py`](modules/dynamic_setup_module.py) | Setup fields whose options are fetched at runtime (`Dynamic`) |
| [`bench_module/`](bench_module/) | Echo module packaged for load tests (Dockerfile, compose, sweep results) |
| [`monitoring/`](monitoring/) | Prometheus + Grafana around a module via gRPC interceptors |

Writing a module is covered in [`docs/module_authoring.md`](../docs/module_authoring.md).
