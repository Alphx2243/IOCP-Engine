# Operations

## Requirements

- Windows 10/11 or Windows Server to run the IOCP server.
- Visual Studio 2022 C++ toolchain.
- CMake 3.20 or newer.
- Node.js, because the CMake file invokes Node while generating schema bindings.

## Build

Windows server build:

```powershell
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release --target iocp_server --parallel
```

Portable non-Windows configure:

```powershell
cmake -S . -B build -DIOCP_ENGINE_BUILD_SERVER=OFF
cmake --build build
```

## Generated files caveat

`CMakeLists.txt` expects generated-binding support files and test/client paths referenced by the README, including:

- `scripts/strip-generated-comments.mjs`
- `scripts/verify_generated.cmake`
- `tests/test_main.cpp`
- `client/src/generated`
- `generated/cpp`

If this checkout does not include those paths, restore them or adjust CMake before expecting the documented `verify_generated`, test, or SDK targets to build.

## Run

```powershell
$env:IOCP_AUTH_TOKEN = "replace-with-a-development-secret"
$env:IOCP_ALLOWED_ORIGIN = "http://localhost:5173"
.\build\Release\iocp_server.exe --port 8080 --metrics-interval-ms 5000
```

The server writes JSON-line startup and metrics events.

## Configuration

Command-line options:

| Option | Effect |
| --- | --- |
| `--port` | Listen port. Default: `8080`. |
| `--workers` | IOCP worker count. Default: hardware concurrency. |
| `--max-connections` | Maximum active connections. |
| `--max-message-bytes` | Maximum FlatBuffers payload size. |
| `--session-queue-bytes` | Maximum queued outbound bytes per session. |
| `--global-queue-bytes` | Maximum queued outbound bytes across sessions. |
| `--auth-token` | Demo shared token. Overrides `IOCP_AUTH_TOKEN`. |
| `--allowed-origin` | Allowed WebSocket `Origin`. Overrides `IOCP_ALLOWED_ORIGIN`. |
| `--metrics-interval-ms` | Metrics print interval. |
| `--thread-affinity` | Pin workers to processor groups when possible. |

Environment variables:

| Variable | Purpose |
| --- | --- |
| `IOCP_AUTH_TOKEN` | Default authentication token. |
| `IOCP_ALLOWED_ORIGIN` | Optional WebSocket origin allow-list value. |

## Metrics

Metrics snapshots include:

- `active_connections`
- `accepted_connections`
- `received_messages`
- `sent_messages`
- `malformed_messages`
- `send_failures`
- `queued_bytes`
- `inbound_bytes`

Startup emits:

```json
{"event":"ready","ready":true,"port":8080}
```

Periodic metrics emit:

```json
{"event":"metrics","active_connections":0,"accepted_connections":0,"received_messages":0,"sent_messages":0,"malformed_messages":0,"send_failures":0,"queued_bytes":0,"inbound_bytes":0}
```

## Shutdown

On Ctrl+C, console close, SIGINT, or SIGTERM, the process requests stop, exits the metrics loop, calls `server.Stop()`, disconnects sessions, drains outstanding operations, stops workers, and prints:

```json
{"event":"server_stopped"}
```

## Current limits

- Authentication is a demo shared token. Use a real identity/session system before production.
- TLS is not implemented in-process. Terminate TLS at a proxy or load balancer.
- Published messages are forwarded as the original client bytes. Add server-issued event envelopes if authoritative sequence IDs or timestamps are required.
- `RoomRegistry` uses one mutex. That is fine until room churn shows up in profiling.

