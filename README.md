# IOCP Engine

High-performance Windows real-time communication engine for sports-facility
updates. The native server is built around Winsock overlapped I/O and IOCP, and
the TypeScript SDK lets browser or Node.js clients consume the same binary
FlatBuffers protocol over WebSocket or raw TCP.

This repository is intended to demonstrate systems design, protocol design,
asynchronous networking, C++ resource management, and client SDK ergonomics in a
single resume-ready project.

## Highlights

- Windows IOCP server using `AcceptEx`, completion ports, owned worker threads,
  stable session handles, async reads/writes, timeout handling, and graceful
  shutdown.
- Strict WebSocket support: HTTP upgrade validation, masking checks, binary
  frames, continuation handling, Ping/Pong, Close frames, and protocol errors.
- Raw TCP transport with a bounded 4-byte big-endian FlatBuffer length prefix.
- Versioned FlatBuffers schema shared by the C++ server and TypeScript SDK.
- Room-based pub/sub for sports-facility domains such as matches, courts,
  equipment, bookings, facilities, and system alerts.
- Backpressure controls through per-session and global queued-byte limits.
- Focused C++ tests for portable protocol utilities and focused SDK tests for
  client state and send backpressure.
- Windows GitHub Actions workflow for CMake, generated-code verification, C++
  tests, and SDK tests.

## Architecture

```text
Browser / Node app
        |
        v
TypeScript SDK
        |
        v
WebSocket or raw TCP
        |
        v
C++ IOCP server
        |
        v
ProtocolRouter
        |
        v
RoomRegistry
        |
        v
Subscribed clients
```

The schema is the contract:

```text
schema/protocol.fbs
        |
        +--> generated/cpp/protocol_generated.h
        |
        +--> client/src/generated/
```

Edit the schema, regenerate bindings, and keep both server and SDK in sync.

## Repository Layout

```text
src/                  C++ server source
src/application/      FlatBuffers message routing and app-level validation
src/core/             IOCP server, sessions, queues, memory helpers
src/protocol/         Raw TCP message framing
src/pubsub/           Room subscription registry
src/transport/        WebSocket handshake and frame parser
schema/               FlatBuffers protocol schema
generated/            Generated C++ protocol bindings
client/               TypeScript SDK and generated TypeScript bindings
tests/                Portable C++ tests
scripts/              Schema generation and verification helpers
docs/                 Architecture, protocol, benchmark, and interview notes
```

## Requirements

- Windows 10/11 or Windows Server for the native IOCP server.
- Visual Studio 2022 C++ toolchain.
- CMake 3.20 or newer.
- Node.js 20 or newer.
- npm.

The portable protocol tests can also be compiled on non-Windows hosts when the
server target is disabled.

## Build And Test

From the repository root:

```powershell
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release --target verify_generated --parallel
cmake --build build --config Release --target iocp_server protocol_tests --parallel
ctest --test-dir build -C Release --output-on-failure
```

Build and test the SDK:

```powershell
cd client
npm ci
npm run verify:generated
npm test
```

On a non-Windows host, configure only the portable pieces:

```powershell
cmake -S . -B build -DIOCP_ENGINE_BUILD_SERVER=OFF
cmake --build build --target protocol_tests
ctest --test-dir build --output-on-failure
```

## Run The Server

```powershell
$env:IOCP_AUTH_TOKEN = "replace-with-a-development-secret"
$env:IOCP_ALLOWED_ORIGIN = "http://localhost:5173"
.\build\Release\iocp_server.exe --port 8080 --metrics-interval-ms 5000
```

The server emits JSON-line startup and metrics events to stdout/stderr.

Useful options:

```text
--port
--workers
--max-connections
--max-message-bytes
--session-queue-bytes
--global-queue-bytes
--auth-token
--allowed-origin
--metrics-interval-ms
--thread-affinity
```

## Protocol Summary

Clients send `NetworkMessage` FlatBuffers envelopes. Raw TCP frames use:

```text
uint32_be payload_length
payload_length bytes of FlatBuffer NetworkMessage
```

WebSocket clients send binary frames containing one `NetworkMessage`. Text
frames are rejected. Client frames must be masked as required by RFC 6455.

Valid room prefixes include:

```text
match:
facility:
court:
equipment:
booking:user:
```

See [docs/protocol.md](docs/protocol.md) for the full protocol notes.

## Documentation

- [Architecture](docs/architecture.md)
- [Protocol](docs/protocol.md)
- [Technical handbook](docs/technical-handbook.md)
- [Benchmark methodology](docs/benchmark.md)
- [Implementation status](docs/implementation-status.md)
- [Interview notes](docs/interview-notes.md)

## Current Limitations

- Authentication is intentionally demo-level: one configured token plus
  authorization hooks. Production use should add JWT/session validation, expiry,
  and a real policy service.
- TLS is expected to terminate in front of the server through infrastructure
  such as nginx, HAProxy, IIS ARR, or a cloud load balancer.
- Broadcasts currently forward the original publish payload. A stronger
  production design would re-wrap server-originated events with authoritative
  sequence IDs and timestamps.
- Existing tests cover important protocol and SDK behavior, but serious
  production rollout should add fuzzing, soak tests, slow-consumer tests,
  failure injection, and full client/server integration tests.

## Resume Pitch

Built a Windows IOCP realtime engine in C++20 with strict WebSocket parsing,
raw TCP framing, FlatBuffers schema generation, room-based pub/sub, backpressure
limits, structured metrics, and a typed TypeScript SDK.
