# BrokerFi Instant Execution Relayer

A Go reference implementation of the BrokerFi session-key smart-account channel,
plus the supporting services used around it on testnet: a perps keeper, a USDC
faucet, a leaderboard API, and a Grok chat endpoint.

The relayer accepts signed channel, session, and execution messages over a
WebSocket, validates them, simulates the resulting contract calls, and submits
sponsored transactions from a relayer hot wallet. The channel contract remains
the source of truth for authorization and replay protection.

> ## Testnet only. Unaudited. Not production-ready.
>
> This repository is intended for experiments and reference on a public testnet.
> It has not been audited or hardened for production. Do not deploy it with
> mainnet funds or use it to custody real user assets.
>
> - **Hot wallets and private keys.** The relayer, keeper, and faucet sign
>   transactions with keys loaded from environment variables. Anyone who can
>   read the process environment or `.env` can take those funds.
> - **Sponsored gas.** The relayer pays gas for submitted transactions. A
>   compromised key, a validation bug, or abusive traffic can drain the gas
>   balance.
> - **Limited authorization checks.** Some channel checks are disabled in
>   development paths, and execution relies on contract-side verification plus
>   simulation. Treat every endpoint as untrusted.
> - **No production controls.** There is no TLS, authentication, durable job
>   queue, production-grade rate limiting, or key-management service here.
>   `/ws` accepts any WebSocket origin, and `/leaderboard` and `/faucet` send
>   permissive CORS headers.
> - **Mnemonic derivation is non-standard.** `RELAYER_MNEMONIC` uses a
>   simplified Keccak-based derivation, not BIP39/BIP44. Prefer a dedicated
>   testnet private key.

## What it does

- **Session-key smart-account channel.** Users get a channel contract from the
  factory. The channel owner signs an EIP-712 `Reconnect` authorization to add a
  session key for a bounded duration; the session key then signs individual or
  batched calls. Channel deployment uses an EIP-712 `Deploy` authorization.
- **Instant execution and batches.** The relayer validates message shape,
  chain ID, expiry, addresses, value fields, and signatures where applicable,
  simulates the channel call, then submits `instantExecuteWithSignature` or the
  batch equivalent and waits for a receipt.
- **Gas estimation.** A `gas_estimation` message returns an estimate for session
  creation so clients can show the sponsored-gas cost.
- **Perps keeper.** A separate bot streams Pyth prices over SSE, tracks active
  positions and limit orders from chain events plus periodic full syncs, and
  executes stop-losses, take-profits, liquidations, and eligible limit orders.
- **Faucet.** A Turnstile-protected testnet USDC faucet that allows one claim per
  wallet, persisted in bbolt.
- **Leaderboard.** Tracks agent-assisted trade counts and computes testnet profit
  from channel USDC balances.
- **Grok chat.** A WebSocket endpoint that proxies chat to xAI, injects the
  configured system prompt, exposes perps tool definitions to the client, and
  runs `request_grok_research` server-side.

The Grok model does not hold keys or submit transactions itself. Tool calls are
streamed to the browser/client, which is responsible for showing confirmations,
executing the tool, and returning the result. Only `request_grok_research` is
executed by this Go server.

Options and Polymarket are not implemented. `grok_config.json` is configured for
perpetual futures only, with `enable_options` and `enable_polymarket` set to
`false`.

## Architecture

```text
Browser / trading client
        |
        |  msgpack over WebSocket :4001/ws       JSON over WebSocket :4001/grok
        v                                              v
  +-------------------+     +----------------+     +------------------+
  | endpoints/        |     | grok/          |     | leaderboard/     |
  | channel/session/  |     | xAI streaming, |     | bbolt stats      |
  | execute handlers  |     | captcha, tools |     +------------------+
  +---------+---------+     +--------+-------+
            |                        |
            v                        v
  +-------------------+     +------------------+
  | relayer/          |     | xAI API          |
  | EIP-712 checks,   |     +------------------+
  | simulation, txs   |
  +---------+---------+
            |
            v
  Testnet RPC  <----  keeper/ (Pyth SSE + order sync + executor)
            ^
            |
  +---------+---------+
  | faucet/           |
  | Turnstile + USDC  |
  +-------------------+
```

### Directory map

| Path | Purpose |
| --- | --- |
| `main.go` | Process entry point: loads env/config, starts the keeper, registers `/ws`, `/faucet`, `/leaderboard`, and `/grok`, serves on `:4001`, handles graceful shutdown. |
| `endpoints/` | WebSocket connection handling and handlers for channel creation, batch channel creation, session creation, instant execution, batch execution, and gas estimation. |
| `relayer/` | EIP-712 domain/signature checks, field validation, contract simulation, gas estimation, transaction submission, receipt waiting, and channel-creation batching. |
| `messagepack/` | MessagePack wire types for the relayer protocol. |
| `keeper/cmd/` | Standalone keeper entry point. |
| `keeper/config/` | Keeper JSON config loading and validation. |
| `keeper/pyth/` | Pyth Hermes SSE price stream and REST update-data client. |
| `keeper/sync/` | On-chain event subscription, periodic order/position sync, optional Goldsky subgraph client. |
| `keeper/orderbook/` | In-memory per-market limit-order books. |
| `keeper/trigger/` | Price-trigger evaluation for stops, take-profits, liquidations, and limit orders. |
| `keeper/executor/` | Transaction construction/signing, retries, and Pyth update attachment for keeper actions. |
| `keeper/metrics/` | Prometheus metrics and HTTP exposition. |
| `faucet/` | `POST /faucet`: Turnstile verification, one-claim-per-wallet tracking, and USDC transfer. |
| `leaderboard/` | `GET /leaderboard`: bbolt trade counts and channel-balance-based testnet profit. |
| `grok/` | xAI streaming client, session/rate limits, Turnstile captcha, prompt loading/filtering, and tool-call routing. |
| `SYSTEM_PROMPT.md` | System prompt for the perps-only Grok assistant. |
| `grok_config.json` | Model, captcha, session, feature, and tool configuration. |
| `keeper_config.example.json` | Placeholder keeper configuration. Copy it to `keeper_config.json` and replace every zero/placeholder address. |
| `.env.example` | Placeholder environment file. Copy it to `.env`. |

### Relayer protocol

Binary WebSocket messages are MessagePack-encoded. The server replies with JSON
envelopes of the form `{"type": "...", "ts": <unix>, "payload": {...}}`.

Supported request types:

| `typ` / `type` | Purpose |
| --- | --- |
| `channel_create` | Deploy a channel for a user with an EIP-712 `Deploy` signature and captcha token. |
| `batch_channel_create` | Deploy channels for up to 50 users in one call. |
| `session_create` | Authorize a session key via the channel's EIP-712 `Reconnect` flow. |
| `instant_execute` | Submit one session-key-signed call to a channel. |
| `batch_instant_execute` | Submit multiple calls in one channel transaction. Optional `via_agent` marks the trade for the leaderboard. |
| `gas_estimation` | Estimate gas for a session creation. |

This is an internal, still-changing protocol, not a stable public API. See
`messagepack/` and `endpoints/handlers.go` for the exact fields.

## Requirements

- Go 1.24.3 or newer (the version in `go.mod`).
- Access to a testnet JSON-RPC endpoint.
- A deployed `InstantExecutionChannelFactory` and the BrokerFi perps contracts
  if you want the keeper to do anything.
- A funded testnet hot wallet for the relayer and keeper.
- A funded testnet USDC wallet for the faucet.
- Cloudflare Turnstile keys if captcha is enabled.
- An xAI API key if the Grok endpoint is enabled.

## Configuration

Copy the example environment file and fill in testnet-only values:

```bash
cp .env.example .env
cp keeper_config.example.json keeper_config.json
```

`.env` is gitignored. Never commit real keys.

### Environment variables

| Variable | Required | Used by | Notes |
| --- | --- | --- | --- |
| `RPC_URL` | Yes | server, keeper, faucet | Testnet RPC endpoint. The server requires it at startup. |
| `FACTORY_ADDRESS` | Yes | server | Channel factory address. |
| `RELAYER_PRIVATE_KEY` | Yes for submission/keeper | relayer, keeper | Hex-encoded hot-wallet key. Without it, some paths only validate/simulate. |
| `RELAYER_MNEMONIC` | Alternative to key | relayer | Takes precedence over `RELAYER_PRIVATE_KEY`; uses simplified, non-BIP39 derivation. Not recommended. |
| `USDC_ADDRESS` | Yes for faucet/leaderboard | faucet, leaderboard | Testnet USDC ERC-20 address. |
| `FAUCET_PRIVATE_KEY` | For faucet | faucet | Funded testnet wallet that sends USDC claims. |
| `CAPTCHA_SECRET` | If captcha enabled | server, faucet, grok | Cloudflare Turnstile secret. |
| `XAI_API_KEY` | For Grok | grok | xAI API key. The endpoint rejects requests without it. |
| `CONFIG_PATH` | Optional | keeper | Keeper JSON path. If unset, the keeper does not start; the HTTP server still runs. |
| `GROK_CONFIG_PATH` | Optional | grok | Defaults to `grok_config.json`. |
| `LEADERBOARD_DB_PATH` | Optional | leaderboard | Defaults to `leaderboard.db`. |
| `LOG_LEVEL` | Optional | server | Defaults to `warn`. |

The HTTP server listens on `:4001` (hard-coded). The keeper's Prometheus
endpoint uses `metrics.prometheus_port` from the keeper config (default `9091`).

### Keeper configuration

`keeper_config.example.json` is a schema-complete example with **placeholder
zero addresses** and public Pyth feed IDs. Replace `contracts.pyth_address`,
`contracts.perps_factory`, and every per-market `trading`, `limit_order_book`,
and `pool` address with the addresses for your testnet deployment. Leave
`sync.goldsky_url` empty to use RPC event sync, or set your own subgraph URL.

`monitored_markets` currently shows ETH-PERP and BTC-PERP. `max_gas_wei` and
`min_profit_usd` are safety limits; tune them for your testnet.

## Build, run, test

```bash
# Download modules
go mod download

# Build everything (server, keeper, packages)
go build ./...

# Build standalone binaries
go build -o bin/relayer .
go build -o bin/keeper ./keeper/cmd

# Run the HTTP server and, if CONFIG_PATH is set, the embedded keeper
cp keeper_config.example.json keeper_config.json   # then edit addresses
CONFIG_PATH=keeper_config.json ./bin/relayer

# Or run directly with the -config flag (it sets CONFIG_PATH)
go run . -config keeper_config.json

# Run only the standalone keeper
go run ./keeper/cmd -config keeper_config.json

# Format, vet, and test
gofmt -w $(find . -name '*.go' -not -path './.git/*')
go vet ./...
go test ./...
```

The HTTP server starts even if the keeper is disabled or fails; a missing
`CONFIG_PATH`, invalid keeper config, or RPC problem leaves the keeper idle while
the other endpoints continue. Check the startup logs for the keeper status. The
keeper logs to stderr and exposes Prometheus metrics on its configured port.

### Tests

The repository includes small unit tests for keeper config validation, Grok
prompt/tool filtering, and relayer message validation. They do not require a
network or a chain:

```bash
go test ./...
```

There are no end-to-end, fork, or integration tests. Build and vet are the main
static checks. Run `go build ./...`, `go vet ./...`, and `go test ./...` before
pushing changes.

## Operational notes

- Run against a disposable testnet only. Fund hot wallets with the minimum gas
  needed and rotate keys after experiments.
- The keeper submits real transactions for stops, take-profits, liquidations,
  and limit orders. Incorrect addresses or market config can cause failed
  transactions or unintended actions. Start with a single market and small
  limits.
- The faucet marks a wallet as claimed in bbolt before sending USDC. If the
  transfer fails, the claim is intentionally left consumed to avoid double
  spends; clear the local database if you need to reset a test environment.
- `leaderboard.db` and `faucet_claims.db` are local runtime state and are
  gitignored.
- Put any publicly reachable deployment behind TLS, request-size limits, and a
  reverse proxy. Do not expose `/ws`, `/faucet`, or `/grok` directly to the
  internet.
- The Grok endpoint is a chat interface, not a trading system of record. Model
  output can be wrong or stale; confirm every trade in the client and rely on
  on-chain state.

## License

MIT. See [LICENSE](LICENSE).

## Security

See [SECURITY.md](SECURITY.md) for reporting and operational security notes.
