# Security Policy

## Project status

This repository is a **testnet-only, unaudited reference implementation**. It is
not production-ready, has not been independently audited, and must not be used
with mainnet funds or real user assets.

## Supported versions

There are no production-supported versions. Security fixes, if any, are applied
to the current `main` branch only. Old snapshots are not maintained.

## Reporting a vulnerability

Please **do not open a public issue** for a suspected vulnerability.

Use GitHub's private vulnerability reporting for this repository
(**Security** tab -> **Report a vulnerability**). If private reporting is not
available, contact the repository maintainers through a private channel and ask
for a secure disclosure address before sharing details.

Please include:

- A description of the issue and the affected component.
- Reproduction steps or a proof of concept.
- The impact you believe is possible (for example, unauthorized transaction
  submission, signature bypass, key exposure, or denial of service).
- Any suggested mitigation.

Never include live private keys, mnemonics, API keys, RPC credentials, or
captcha secrets in a report. Redact them.

## Operational security notes

- The relayer, keeper, and faucet use **hot wallets** loaded from environment
  variables. Treat the host and its environment as sensitive.
- Sponsored gas means a compromised relayer key can drain its gas balance and
  submit transactions until the key is rotated.
- `RELAYER_MNEMONIC` derivation in this codebase is a simplified,
  non-BIP39/BIP44 scheme. Prefer an explicitly generated private key for any
  test deployment, and never reuse a wallet that has held mainnet assets.
- The channel creation and captcha checks have development-mode gaps. Review
  the code before exposing any endpoint to an untrusted network.
- The `/ws`, `/faucet`, `/leaderboard`, and `/grok` endpoints have no built-in
  TLS, authentication layer, or production-grade rate limiting. Put them
  behind an appropriately configured reverse proxy if they must be reachable
  beyond localhost.

## Scope

In scope: this repository's Go code, configuration examples, and documentation.
Out of scope: the BrokerFi contracts and frontend, third-party dependencies, and
any deployment infrastructure not contained in this repository.
