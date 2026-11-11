# Tests

Cross-node tests. Tests of a single node live in that node (e.g. [nodes/ingestor/tests/](../nodes/ingestor/tests/)), with the same rule for fixtures: small and synthetic.

| Path | Content |
|---|---|
| `unit/` | Unit tests per package and node |
| `point_in_time/` | Leakage tests: no feature for instant *t* may use data with `available_at > t` (DG-D03) |
| `e2e/` | End-to-end tests of the full loop |
| `fixtures/` | **Small, synthetic** test data only — never real market dumps |

Status: **empty**.
