# Contributing to Hermes

Thank you for your interest in contributing to Hermes! As a high-performance event streaming engine written in Rust, we aim for the highest standards of safety, speed, and reliability.

This guide outlines the process of contributing to Hermes and helps you get your development environment set up.

---

## 🗺️ Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct:
*   Be respectful, welcoming, and inclusive to everyone.
*   Focus on constructive feedback and collaboration.
*   Prioritize technical merit, security, and performance.

---

## 🔍 How Can I Contribute?

### 🐛 Reporting Bugs
Before opening a bug report, please check existing issues to ensure it hasn't already been reported. If you find a new bug:
1.  Open an **Issue** on GitHub.
2.  Provide a clear description of the behavior.
3.  Include reproduction steps, OS details, Rust version, and log outputs.

### 💡 Suggesting Enhancements
We welcome ideas to extend Hermes! To suggest an enhancement:
1.  Open an **Issue** to discuss the feature request before writing code.
2.  Explain the use case, desired behavior, and potential API design.

### 🛠️ Submitting Pull Requests
If you are ready to write code:
1.  Fork the repository and clone it locally.
2.  Create a branch from `main` (e.g. `feature/my-new-feature` or `fix/issue-id`).
3.  Implement your changes and verify formatting, lints, and tests.
4.  Push your branch to your fork and submit a Pull Request to our `main` branch.

---

## 💻 Developer Setup

### 1. Prerequisites
Ensure you have the Rust toolchain installed:
*   [Rustup](https://rustup.rs/) (stable channel)
*   `cargo-fmt` (for formatting code)
*   `clippy` (for linting)

### 2. Working with the Code
Clone your fork and navigate into the folder:
```bash
git clone https://github.com/your-username/hermes.git
cd hermes
```

Check that the project compiles and all integration tests pass on your machine:
```bash
cargo check
cargo test
```

---

## 📝 Code Standards

To keep the codebase maintainable and robust, we enforce the following guidelines:

### 1. Formatting
All Rust code must be formatted using the official rustfmt tool. Run this before making a commit:
```bash
cargo fmt --all -- --check
```

### 2. Linting
All code must be free of warnings. Run Clippy to detect common issues and code smells:
```bash
cargo clippy --all-targets -- -D warnings
```

### 3. Testing
*   **Unit Tests**: Put these in the source files under standard `mod tests` blocks.
*   **Integration Tests**: Add new integration tests to [`tests/integration_tests.rs`](file:///C:/Users/vboxuser/CLionProjects/Hermes/tests/integration_tests.rs) for complex multi-node orchestration, crash recovery, transaction handling, or client network interfaces.
*   Make sure all tests clean up their storage artifacts on completion using a `Drop` guard.

### 4. Git Commit Messages
We follow the [Conventional Commits](https://www.conventionalcommits.org/) convention. Please structure your commit messages like this:
```text
<type>(<scope>): <short summary>

[optional body]
```
Common types:
*   `feat`: A new feature (e.g. `feat(wal): add async flush policy`)
*   `fix`: A bug fix (e.g. `fix(protocol): resolve CRC32 checksum mismatch`)
*   `docs`: Documentation changes (e.g. `docs(readme): document CLI consumer-group options`)
*   `perf`: Performance improvements
*   `refactor`: Code changes that do not fix bugs or add features

---

## ⚡ PR Review Process

1.  **CI Validation**: Once you open a PR, GitHub Actions will automatically run the suite of tests, formatting checks, and clippy lints.
2.  **Review**: A maintainer will review your code changes, focus on memory safety, correct behavior of concurrent components (like `parking_lot::Mutex` or `dashmap::DashMap`), and performance impacts.
3.  **Merge**: Once approved and status checks pass, your changes will be merged into the master `main` branch.
