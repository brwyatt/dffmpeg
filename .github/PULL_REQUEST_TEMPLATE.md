## Description
Please include a summary of the change and which issue is fixed. Please also include relevant motivation and context. List any dependencies that are required for this change.

Fixes # (issue)

## Type of change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Documentation update
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)

## Breaking Changes
- If this is a breaking change, please describe the migration path or dual-mode operation plan.

## Checklist:

### Code Quality
- [ ] My code follows the style guidelines of this project
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] I have checked that my changes do not introduce new linting errors

### Testing
- [ ] I have added tests for critical code paths and functionality
- [ ] New and existing unit tests pass locally with my changes

### Architecture & Philosophy
- [ ] **Async/Await:** Verified all I/O is non-blocking.
- [ ] **Database:** Verified proper handling of database types across engines (e.g., ULID storage as TEXT in SQLite).
- [ ] **Statelessness:** Ensured changes align with stateless/path-blind philosophy.
- [ ] **Plugins:** If adding a new plugin, confirmed it belongs in the core repo (vs external package).
- [ ] **Models:** Used Pydantic V2 for models/config.
