---
name: Feature request
about: Suggest an idea for this project
title: ''
labels: enhancement
assignees: ''

---

**Which component(s) are affected?**
- [ ] `dffmpeg-coordinator`
- [ ] `dffmpeg-worker`
- [ ] `dffmpeg-client`
- [ ] `dffmpeg-common`
- [ ] `docs`

**Is your feature request related to a problem? Please describe.**
A clear and concise description of what the problem is. Ex. I'm always frustrated when [...]

**Describe the solution you'd like**
A clear and concise description of what you want to happen.

**Philosophy Alignment**
- Does this request align with the "stateless" architecture (e.g. can the coordinator be restarted without losing in-progress work or messages)?
- Does this support the plugin architecture (e.g. adding a new transport or DB engine)?
- Does this feature maintain/support the coordinator's "neutrality" between clients and workers (e.g. the coordinator's "path-blindness")?

**Scope Check: Core vs Plugin**
- If this is a new Transport or DB plugin, explain why it should be maintained in the core repository rather than as an independent package.

**Impact Analysis**
- If this requires a breaking change, how do you envision the migration path or backward compatibility plan?

**Describe alternatives you've considered**
A clear and concise description of any alternative solutions or features you've considered.

**Additional context**
Add any other context or screenshots about the feature request here.
