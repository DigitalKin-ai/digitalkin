# DigitalKin Python SDK

Welcome to the DigitalKin Python SDK, a powerful tool designed for developers who aim to build and manage agents within multi-agent systems according to the innovative DigitalKin agentic mesh standards. This SDK streamlines the process of creating and managing custom Tools, Triggers, and Kin Archetypes while ensuring full compliance with the DigitalKin ecosystem's standards.

## 🚀 Features

- **Seamless Integration**: Easily integrate with DigitalKin's services using our comprehensive gRPC support.
- **Customizable Agents**: Build custom agents and manage their lifecycle efficiently.
- **Standards Compliance**: Adhere to the latest DigitalKin agentic mesh standards.
- **Robust Development Tools**: Utilize advanced development tools for testing, building, and deploying your projects.

## 📦 Installation

To install the DigitalKin SDK, simply run:

```bash
pip install digitalkin
```

**Optional Taskiq Integration**: Asynchronous task execution powered by Taskiq, backed by RabbitMQ and Redis To enable the Rabbitmq streaming capabilities, run:

```sh
sudo rabbitmq-plugins enable rabbitmq_stream

# Core + Taskiq integration (RabbitMQ broker)
pip install digitalkin[taskiq]
```

## 🛠️ Usage

### Basic Import

Start by importing the necessary modules:

```python
import digitalkin
```

## Features

### Taskiq with RabbitMQ

TaskIQ intergration allows the module to scale for heavy CPU tasks by having the request's stateless module in a new instance.

- **Decoupled Scalability**: RabbitMQ brokers messages, letting producers and consumers scale independently.
- **Reliability**: Durable queues, acknowledgements, and dead-lettering ensure tasks aren’t lost.
- **Concurrency Control**: Taskiq’s worker pool manages parallel execution without custom schedulers.
- **Flexibility**: Built-in retries, exponential backoff, and Redis result-backend for resilient workflows.
- **Ecosystem**: Battle-tested `aio-pika` AMQP client plus Taskiq’s decorator-based API.

By combining Taskiq’s async API with RabbitMQ’s guarantees, you get a robust, production-ready queue with minimal boilerplate.

## 👷‍♂️ Development

### Prerequisites

Ensure you have the following installed:

- Python 3.10+
- [uv](https://astral.sh/uv) - Modern Python package management
- [buf](https://buf.build/docs/installation) - Protocol buffer toolkit
- [protoc](https://grpc.io/docs/protoc-installation/) - Protocol Buffers compiler
- [Task](https://taskfile.dev/) - Task runner

### Setting Up Your Development Environment

Clone the repository and set up your environment with these commands:

```bash
# Clone the repository with submodules
git clone --recurse-submodules https://github.com/DigitalKin-ai/digitalkin.git
cd digitalkin

# Setup development environment
task setup-dev
source .venv/bin/activate
```

### Common Development Tasks

Utilize the following commands for common tasks:

```bash
# Build the package
task build-package

# Run tests
task run-tests

# Format code using Ruff linter and formatter
task linter

# Clean build artifacts
task clean

# Bump version (major, minor, patch)
task bump-version -- major|minor|patch
```

### Publishing Process

1. Update code and commit changes. (following conventional branch/commit standard)
1. Use `task bump-version -- major|minor|patch` command to commit new version.
1. Use GitHub "Create Release" workflow to publish the new version.
1. Workflow automatically publishes to Test PyPI and PyPI.

## 📄 License

This project is licensed under the terms specified in the LICENSE file.

______________________________________________________________________

For more information, please visit our [Homepage](https://github.com/DigitalKin-ai/digitalkin), check our [Documentation](https://github.com/DigitalKin-ai/digitalkin), or report issues on our [Issues page](https://github.com/DigitalKin-ai/digitalkin/issues).

Happy coding! 🎉🚀
