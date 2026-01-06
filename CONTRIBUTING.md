# Contributing to EloqStore

Thank you for your interest in contributing to EloqStore! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## How to Contribute

### Reporting Bugs

If you find a bug, please create an issue with the following information:

- **Clear description** of the bug
- **Steps to reproduce** the issue
- **Expected behavior** vs **actual behavior**
- **Environment details** (OS, compiler version, etc.)
- **Minimal code example** that reproduces the issue (if applicable)
- **Logs or error messages** (if applicable)

### Suggesting Enhancements

We welcome suggestions for improvements! When proposing an enhancement:

- **Clear description** of the proposed enhancement
- **Use case** - explain why this enhancement would be useful
- **Proposed solution** (if you have one)
- **Alternatives considered** (if any)

### Contributing Code

#### Getting Started

1. **Fork the repository** and clone your fork
2. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

#### Development Setup

1. **Build the project** (see [README.md](README.md) for build instructions):
   ```bash
   mkdir build
   cd build
   cmake .. -DCMAKE_BUILD_TYPE=Debug
   cmake --build . -j8
   ```

2. **Run tests** to ensure everything works:
   ```bash
   ctest --test-dir build/tests/
   ```

#### Coding Standards

- **Follow existing code style** - Please match the style of the existing codebase
- **Write clear, self-documenting code** - Use meaningful variable and function names
- **Add comments** for complex logic or non-obvious behavior
- **Keep functions focused** - Each function should have a single, clear responsibility
- **Handle errors appropriately** - Don't ignore error conditions

#### C++ Specific Guidelines

- Use modern C++ features where appropriate
- Prefer `const` correctness
- Use smart pointers (`std::unique_ptr`, `std::shared_ptr`) appropriately
- Follow RAII principles
- Avoid raw pointers when possible
- Use meaningful namespaces

#### Testing

- **Add tests** for new features or bug fixes
- **Ensure all tests pass** before submitting
- **Update existing tests** if your changes affect them
- **Test edge cases** and error conditions

#### Commit Messages

Write clear, descriptive commit messages:

- Use [conventional commit messages](https://www.conventionalcommits.org/en/) as pull request titles. Examples:
    * New feature: `feat: adding foo API`
    * Bug fix: `fix: issue with foo API`
    * Documentation change: `docs: adding foo API documentation`
- Keep the first line under 72 characters
- Provide additional context in the body if needed
- Reference issue numbers if applicable

Example:
```
feat: add support for batch write operations

Implements efficient batch writing to improve throughput for
bulk insert operations. Includes tests and documentation.

Fixes #123
```

#### Submitting Changes

1. **Ensure your code compiles** without warnings
2. **Run all tests** and ensure they pass
3. **Update documentation** if needed (README, code comments, etc.)
4. **Push your changes** to your fork
5. **Create a Pull Request** with:
   - Clear title and description
   - Reference to related issues (if any)
   - Summary of changes
   - Testing performed

#### Pull Request Process

1. **Wait for review** - Maintainers will review your PR
2. **Address feedback** - Make requested changes and update your PR
3. **Keep PRs focused** - One feature or fix per PR
4. **Keep PRs up to date** - Rebase on main if needed

### Documentation

- **Update README.md** if you add new features or change build/usage instructions
- **Add code comments** for public APIs
- **Update examples** if you change API behavior

### Project Structure

- `include/` - Header files
- `src/` - Source files
- `tests/` - Test files
- `examples/` - Example code
- `benchmark/` - Benchmark code
- `tools/` - Utility tools

### Questions?

If you have questions about contributing, please:

- Open an issue with the `question` label
- Check existing issues and discussions
- Review the codebase and documentation

## Recognition

Contributors will be recognized in the project's documentation and release notes. Thank you for helping make EloqStore better!

---

**Note**: By contributing to EloqStore, you agree that your contributions will be licensed under the Business Source License 2.0, the same license that covers the project.

