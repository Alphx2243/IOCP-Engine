# Architecture

IOCP Engine is a Windows C++20 real-time messaging server. It accepts raw TCP and WebSocket clients on the same listener, decodes FlatBuffers `NetworkMessage` payloads, and routes authenticated room publish/subscribe traffic.

```text
client socket
  |
  v
IocpServer
  |
  +-- transport detection
  |     +-- "GET " prefix -> WebSocket upgrade
  |     +-- otherwise    -> raw TCP
  |
  +-- frame decoder
  |     +-- WebSocketDecoder
  |     +-- LengthPrefixedAccumulator
  |
  v
ProtocolRouter
  |
  +-- FlatBuffers verification
  +-- authentication
  +-- room join/leave/publish validation
  |
  v
RoomRegistry
  |
  v
IocpServer::Send recipients
```

## Main components

### `IocpServer`

`IocpServer` owns the socket listener, IO completion port, accept/read/write operations, sessions, worker threads, timer thread, metrics, and shutdown.

On Windows it uses:

- Winsock overlapped sockets.
- `AcceptEx` for asynchronous accepts.
- `CreateIoCompletionPort` / `GetQueuedCompletionStatus` for worker dispatch.
- Per-session queues for ordered writes.
- A timer loop for handshake, idle, close, and WebSocket ping/pong timeouts.

On non-Windows hosts, the server implementation is a stub that reports IOCP as unavailable. Portable components can still be built when the server target is disabled.

### Transport detection

New sessions start in `TransportKind::Detecting`.

- If the first bytes can be an HTTP `GET ` request, the server waits for a WebSocket upgrade request.
- Otherwise, the buffered bytes are treated as raw TCP framing data.

After detection, the session moves to `Open` and all later input goes through the selected decoder.

### WebSocket transport

`WebSocketTransport::ParseHandshake` validates the HTTP/1.1 upgrade request, required WebSocket headers, optional origin policy, and `Sec-WebSocket-Key`.

`WebSocketDecoder` accepts binary messages plus ping, pong, and close control frames. It rejects text frames, unmasked client frames, invalid RSV/opcodes, oversized messages, invalid close payloads, and malformed fragmentation.

### Raw TCP transport

`LengthPrefixedAccumulator` decodes this format:

```text
uint32_be payload_length
payload_length bytes of FlatBuffers NetworkMessage
```

Zero-length frames, oversized frames, and buffered-byte limit violations are rejected.

### `ProtocolRouter`

`ProtocolRouter` verifies each FlatBuffer envelope before reading it. It supports:

- `Authenticate`
- `Ping`
- `Pong`
- `JoinRoom`
- `LeaveRoom`
- `Publish`

Server-to-client payloads sent by clients are rejected with `FORBIDDEN`.

### `RoomRegistry`

`RoomRegistry` keeps both room-to-session and session-to-room indexes. A publish is allowed only when:

1. the room name is valid;
2. the session is authorized for that room;
3. the session is already subscribed to that room.

On disconnect, all room memberships for the session are removed.

## Concurrency model

- Worker threads process IOCP completions.
- Session lookup is sharded to reduce lock contention.
- Each session has a send mutex and ordered send queue.
- Room membership is protected by one mutex.
- Authenticated principals are protected by one mutex.

That is intentionally simple. If room churn or auth lookups become hot, the next likely split is sharding `RoomRegistry` by room name.

## Backpressure and limits

The server enforces limits at multiple layers:

- maximum connections;
- maximum message size;
- maximum per-session inbound buffered bytes;
- maximum global inbound buffered bytes;
- maximum per-session queued outbound bytes;
- maximum global queued outbound bytes;
- maximum messages per second per session;
- malformed message limit before close.

Outbound data is framed at send time according to the session transport: raw TCP gets a 4-byte length prefix, WebSocket gets a binary frame header.

