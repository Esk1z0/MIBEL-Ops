# Infrastructure

Cross-node infrastructure. See ADR-01 and ADR-02 in [docs/06_technical_decisions.md](../docs/06_technical_decisions.md) and US-02, US-13 in [docs/02_user_stories.md](../docs/02_user_stories.md).

| Path | Content |
|---|---|
| `join-node.sh` | Node onboarding by role (`--role compute|ingestor`) |
| `vpn/` | Tailscale ACL templates (**no keys or real hostnames**) |
| `cloud/` | Generic, provider-agnostic VM template (cloud-init) for ephemeral compute nodes |

Status: **empty**. Nothing here may contain secrets, IPs or hostnames (see [AGENTS.md](../AGENTS.md) §3).
