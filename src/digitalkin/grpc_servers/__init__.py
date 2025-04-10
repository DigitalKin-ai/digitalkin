"""This package contains the gRPC server and client implementations.

```shell
digitalkin/grpc/
├── __init__.py
├── base_server.py        # Base server implementation with common functionality
├── module_server.py      # Module-specific server implementation
├── registry_server.py    # Registry-specific server implementation
├── module_servicer.py    # gRPC servicer for Module service
├── registry_servicer.py  # gRPC servicer for Registry service
├── client/               # Client libraries for connecting to servers
│   ├── __init__.py
│   ├── module_client.py
│   └── registry_client.py
└── utils/                # Utility functions
    ├── __init__.py
    └── server_utils.py   # Common server utilities
```
"""
