# AGENTS.md — Rules for AI agents in MIBEL-Ops

> Applies to any agent (Claude Code, Copilot, Codex…) working in this repository.
> Project context: [docs/README.md](docs/README.md) · Decisions: [docs/06_technical_decisions.md](docs/06_technical_decisions.md)

## 1. General principle
The repository is **public** and the system runs on **personal machines**. When in doubt about security, data or git: **stop and ask**.

## 2. Language
- **Everything stored in the repository is written in English**: documentation, code, comments, configuration, file and folder names, commit messages, branch names, PR descriptions and issues.
- **Replies to the maintainer are written in Spanish.**

## 3. Data security
- **Never** write secrets to versioned files: tokens (ESIOS, ENTSO-E, AEMET), passwords, Tailscale/WireGuard keys, `kubeconfig`, K3s tokens. They go in `.env` (ignored) or Kubernetes *Secrets*; `.env.example` only contains placeholders (`ESIOS_TOKEN=<your-token-here>`).
- **Never** commit personal or infrastructure data: emails, VPN IPs or hostnames, personal paths, names of friends' nodes.
- **Data does not go into Git.** Market data, Parquet files and model artifacts are versioned with **DVC** and **MLflow** (see [.gitignore](.gitignore)). Git only holds `*.dvc` files and small, synthetic fixtures in `tests/fixtures/`.
- Do not put tokens in URLs, logs or error messages. Respect the usage limits of external APIs.
- Treat any downloaded file (OMIE, APIs) as **untrusted**: never execute it; read it only as data.

## 4. System security
The following require **explicit confirmation** from the maintainer every time:
- Destructive commands: `rm -rf`, `DROP`/`TRUNCATE`, `dvc gc`, `mlflow gc`, `docker volume prune`, `kubectl delete`, the real purge of `retention_flow` (default is `--dry-run` only).
- Acting on **real infrastructure**: nodes (SSH), the K3s cluster, the VPN, the manager's DB, the MLflow registry (promotions, aliases, rollback).
- Installing software on the system or changing operating-system configuration.
- **Quality**: changing a threshold or a mode (`block`/`warn`/`off`) of the `production` profile in `config/quality/`, or setting a criterion's status to *Fixed*. Also add the corresponding entry to the changelog of the document in [docs/quality/](docs/quality/README.md).

By default, work locally and with the `development` profile.

## 5. Git: saving rules
### 5.1 Always ask for permission
**No operation that saves or alters history is performed without explicit permission**, even if a similar one was approved before:
`commit` (checkpoint), `branch` / `checkout -b`, `merge`, `rebase`, `tag`, `stash`, `push`, `pull`, `reset`, `revert`, `cherry-pick`, `remote` changes and GitHub operations (PRs, issues, releases).

Before asking permission for a commit, the agent:
1. shows `git status` and a summary of the `diff`;
2. checks that no secrets or data are among the staged changes;
3. runs lint and tests if they exist, and reports the result;
4. proposes **the branch, the message and the list of files**, and waits for the answer.

**Forbidden without exception:** `push --force` to `main`, rewriting already-published history, and `--no-verify`.

### 5.2 Branches
| Branch | Use |
|---|---|
| `main` | Always stable; integrated through PRs once a remote exists |
| `feat/<scope>-<topic>` | New functionality (e.g. `feat/ingestor-esios`) |
| `fix/<scope>-<topic>` | Bug fix |
| `docs/<topic>` | Documentation only |
| `chore/<topic>` | Configuration, CI, dependencies |
| `exp/<topic>` | Model experiments (may never be merged) |

### 5.3 Commits
- *Conventional Commits* format: `type(scope): description in English, imperative mood`.
- Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `perf`.
- Scopes: `common`, `ingestor`, `manager`, `compute`, `infra`, `config`, `quality`, `docs`.
- One commit = one logical change. If it touches the architecture or a decision, the message body links the affected ADR or document.

## 6. Mandatory reply after any change
Every reply in which something was created, modified or deleted must include:
1. **What was done and why**, in a few lines.
2. **Affected files**, with a relative link and, where relevant, the section: created, modified and deleted (e.g. `docs/quality/02_service_baseline.md §3`).
3. **Reference documents** that justify or are affected by the change (requirement, user story, ADR, quality criterion).
4. **What was not done or not verified** (tests not run, `[VERIFY]` items, pending decisions).

## 7. Consistency with the documentation
- If the code departs from what is documented in `docs/`, update the document in the same change or flag it explicitly.
- Any new or modified decision is recorded in [docs/06_technical_decisions.md](docs/06_technical_decisions.md) (ADR-00).
