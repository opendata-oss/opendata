# Contributing to OpenData

Thank you for your interest in contributing to OpenData! We welcome contributions from the community.

## Table of Contents

- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [RFCs](#rfcs)
- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)

## Getting Started

OpenData is a Rust workspace containing multiple crates:

- `common` - Shared components and utilities
- `tsdb` - Object storage-native timeseries database
- `log` - Object storage-native event streaming backend

Before contributing, we recommend reading the [README](README.md) to understand the project architecture and looking at existing issues and pull requests.

## How to Contribute

### Reporting Bugs

Open an issue with:

- Steps to reproduce the issue
- Expected vs actual behavior
- Your environment (OS, Rust version)
- Relevant logs or error messages

### Suggesting Features

Open an issue describing:

- The feature and the problem it solves
- Any implementation ideas (optional)

### Contributing Code

1. For significant changes, open an issue or RFC first to discuss your approach
2. Fork the repository and create a branch for your work
3. Write code and tests
4. Submit a pull request

A maintainer will review the PR as soon as we have availability! 

## RFCs

Major changes require an RFC (Request for Comments) before implementation. This includes:

- New database types
- Changes to the storage, query, or metadata layers
- New APIs or breaking API changes
- Architectural changes affecting multiple crates
- New dependencies on external systems

### RFC Process

1. Copy the [RFC template](rfcs/0000-template.md) to the appropriate directory:
   - `rfcs/` - For cross-project or architectural changes
   - `<project>/rfcs/` - For project-specific changes (e.g., `timeseries/rfcs/`)
2. Name the file `NNNN-short-description.md` where `NNNN` is the next available number
3. Fill out the template sections:
   - **Summary** - Brief description of the proposal
   - **Motivation** - The problem being solved and why it matters
   - **Design** - Detailed explanation of the proposed solution
   - **Alternatives** - Other approaches considered and why they were rejected
   - **Open Questions** - Unresolved issues for discussion
4. Submit a pull request with the RFC
5. Maintainers and community members will provide feedback on the PR
6. Once the RFC is approved by two maintainers and merged, implementation can begin
7. Reference the RFC in any implementing PRs

For bug fixes, documentation, and minor features, skip the RFC and go straight to a PR.

## Development Setup

### Prerequisites

- Rust stable toolchain ([rustup](https://rustup.rs/))
- Git

### Building

```bash
git clone https://github.com/opendata-oss/opendata.git
cd opendata

cargo build --all-targets
cargo test --all
```

## Code Style

### Formatting

Run `cargo fmt` before committing:

```bash
cargo fmt --all
```

### Linting

All code must pass clippy with no warnings:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Guidelines

- Follow Rust naming conventions
- Write clear, self-documenting code
- Add comments for complex logic
- Prefer returning `Result` for fallible operations

## Testing

Include tests for new functionality.

### Test Style

Use the `should_` prefix and given/when/then pattern:

```rust
#[test]
fn should_return_error_when_input_is_invalid() {
    // given
    let input = "invalid";

    // when
    let result = process(input);

    // then
    assert!(result.is_err());
}
```

### Running Tests

```bash
cargo test --all                    # Run all tests
cargo test --all -- --nocapture     # Run with output
cargo test -p timeseries             # Run tests for a specific crate
```

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
