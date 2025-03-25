# DigitalKin Python SDK

[![CI](https://github.com/DigitalKin-ai/digitalkin/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalKin-ai/digitalkin/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/digitalkin.svg)](https://pypi.org/project/digitalkin/)
[![Python Version](https://img.shields.io/pypi/pyversions/digitalkin.svg)](https://pypi.org/project/digitalkin/)
[![License](https://img.shields.io/github/license/DigitalKin-ai/digitalkin)](https://github.com/DigitalKin-ai/digitalkin/blob/main/LICENSE)

Welcome to the DigitalKin Python SDK, a powerful tool designed for developers
who aim to build and manage agents within multi-agent systems according to the
innovative DigitalKin agentic mesh standards. This SDK streamlines the process
of creating and managing custom Tools, Triggers, and Kin Archetypes while
ensuring full compliance with the DigitalKin ecosystem's standards.

## 🚀 Features

- **Seamless Integration**: Easily integrate with DigitalKin's services using
  our comprehensive gRPC support.
- **Customizable Agents**: Build custom agents and manage their lifecycle
  efficiently.
- **Standards Compliance**: Adhere to the latest DigitalKin agentic mesh
  standards.
- **Robust Development Tools**: Utilize advanced development tools for testing,
  building, and deploying your projects.

## 📦 Installation

To install the DigitalKin SDK, simply run:

```bash
pip install digitalkin
```

## 🛠️ Usage

### Basic Import

Start by importing the necessary modules:

```python
import digitalkin
```

## 👷‍♂️ Development

### Prerequisites

Ensure you have the following installed:

- Python 3.10+
- [uv](https://astral.sh/uv) - Modern Python package management
- [buf](https://buf.build/docs/installation) - Protocol buffer toolkit
- [protoc](https://grpc.io/docs/protoc-installation/) - Protocol Buffers
  compiler
- [Task](https://taskfile.dev/) - Task runner

### Setting Up Your Development Environment

Clone the repository and set up your environment with these commands:

```bash
# Clone the repository with submodules
git clone --recurse-submodules https://github.com/DigitalKin-ai/digitalkin.git
cd digitalkin

# Setup development environment
task setup-dev
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

1. Update code and commit changes. (following conventional branch/commit
   standard)
2. Use `task bump-version -- major|minor|patch` command to commit new version.
3. Use GitHub "Create Release" workflow to plublish the new version.
4. Workflow automatically publishes to Test PyPI and PyPI.

## 📄 License

This project is licensed under the terms specified in the LICENSE file.

---

For more information, please visit our
[Homepage](https://github.com/DigitalKin-ai/digitalkin), check our
[Documentation](https://github.com/DigitalKin-ai/digitalkin), or report issues
on our [Issues page](https://github.com/DigitalKin-ai/digitalkin/issues).

Happy coding! 🎉🚀
