# Protocol

The wire contract is `schema/protocol.fbs`. All application messages are FlatBuffers `NetworkMessage` envelopes with `protocol_version = 1`.

## Transports

### Raw TCP

Each raw TCP message is length-prefixed:

```text
uint32_be payload_length
payload_length bytes of FlatBuffers NetworkMessage
```

`payload_length` must be greater than zero and no larger than the configured `--max-message-bytes`.

### WebSocket

WebSocket clients send one binary FlatBuffers `NetworkMessage` per message. Text messages are rejected.

Client frames must be masked. Server frames are not masked. Ping, pong, close, continuation, and binary frames are handled; invalid control frames and invalid fragmentation close the connection.

## Envelope

`NetworkMessage` fields:

| Field | Purpose |
| --- | --- |
| `protocol_version` | Must be `1`. |
| `request_id` | Client request correlation. Server responses copy it when applicable. |
| `sequence_id` | Client or server sequence value. Server-generated responses use a server counter. |
| `sent_at_ms` | Unix timestamp in milliseconds. |
| `payload` | One `Payload` union value. |

## Client-to-server payloads

### `Authenticate`

Authenticates the session with a token. The demo executable compares against `--auth-token` or `IOCP_AUTH_TOKEN`.

Response: `AuthenticationResult`.

### `Ping`

Application-level ping. The server responds with `Pong` and echoes `timestamp_ms`.

### `Pong`

Accepted. This is separate from WebSocket protocol-level pong handling.

### `JoinRoom`

Requires authentication. On success, subscribes the session to the room.

Response: `SubscriptionAck`.

### `LeaveRoom`

Removes the session from the room.

Response: `SubscriptionAck`.

### `Publish`

Requires authentication, a valid room, valid domain payload, authorization for publishing, and an existing subscription to that room.

On success, the original `NetworkMessage` bytes are sent to every current subscriber in the room snapshot.

## Server-to-client payloads

These are rejected when sent by clients:

- domain events: `MatchInfo`, `MatchScoreUpdate`, `FacilityUpdate`, `EquipmentUpdate`, `BookingNotification`, `SystemAlert`
- `AuthenticationResult`
- `SubscriptionAck`
- `ErrorResponse`

## Rooms

Valid room prefixes:

- `match:`
- `facility:`
- `court:`
- `equipment:`
- `booking:user:`

Room names must be 1-160 characters and may contain only ASCII letters, digits, `:`, `-`, `_`, and `.`.

By default, authenticated sessions may access all non-booking rooms. A `booking:user:<id>` room is only allowed when `<id>` equals the authenticated principal. Embedders can pass a custom `RoomAuthorizer` to override this policy.

## Domain payload validation

Published domain payloads are validated before broadcast:

- IDs and sport names are capped at 160 characters.
- Display names are capped at 256 characters.
- Free text is capped at 4096 characters.
- `MatchInfo` requires two different team names.
- `FacilityUpdate` allows up to 1024 courts, all matching the parent facility ID.
- `EquipmentUpdate` allows up to 4096 inventory items, all matching the parent facility ID, with `available_count <= total_inventory`.
- `BookingNotification` requires `start_time_ms < end_time_ms`.
- `SystemAlert.message` is required.

## Error behavior

Malformed FlatBuffers, missing payloads, bad versions, invalid rooms, unauthorized access, forbidden payloads, and invalid publish payloads produce an `ErrorResponse` when the connection is still usable.

Repeated malformed application messages are counted by the server; reaching the configured malformed-message limit closes the session.

