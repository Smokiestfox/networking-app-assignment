# Protocol Specification

## 1. Transport Overview (TCP 9000 / UDP 9001)

Core collaboration traffic uses raw sockets.

- **TCP port 9000**: Control channel for server hello, authentication, and liveness. Uses length-prefix framing to preserve message boundaries over TCP byte streams.
- **UDP port 9001**: Data channel for heartbeat, user discovery, real-time messaging, and file chunk transfer with application-layer reliability.

## 2. TCP Framing (Length-Prefix)

TCP is a stream-oriented transport protocol with no built-in message boundaries. Without framing, consecutive writes can suffer from packet coalescing (sticky packets) or network fragmentation, causing multiple messages to merge or split across `recv()` calls.

To ensure deterministic message parsing, all control traffic over TCP is enclosed in a length-prefixed frame:

```
+------------------------+------------------------------------+
|  Length (4 bytes, !I)  |  Payload data (Length bytes)       |
+------------------------+------------------------------------+
```

- **Length prefix (4 bytes)**: Unsigned 32-bit integer in network byte order (Big-Endian, struct format `!I`). Specifies the exact length of the payload that follows.
- **Payload**: UTF-8 encoded JSON object (e.g. `HELLO`, `AUTH`, `AUTH_OK`, `PING`, `PONG`).
- **Framing mechanism**:
  - **Sender (`send_frame`)**: Computes `len(payload)`, packs the 4-byte header, and sends `header + payload` via `sendall()`.
  - **Receiver (`recv_frame`)**: Reads exactly 4 bytes using a looped `recv()` (`recv_exact`) to decode the expected payload size, then reads exactly that number of bytes before handing the buffer to the JSON decoder. Returns `None` on clean socket closure.

## 3. Packet Format (UDP / Core Protocol)

UDP datagrams share a fixed 37-byte binary header followed by a variable-length payload. Fields are encoded in network byte order (Big-Endian).

Struct format: `!2sBBB II 16s H H I` (Total header size: 37 bytes).

### 3.1 Header Fields

| Field | Size | Type | Description | Example |
| --- | ---: | :---: | --- | --- |
| Magic | 2 bytes | `2s` | Protocol identifier, fixed to `NP` | `b"NP"` |
| Version | 1 byte | `B` (uint8) | Protocol version | `1` |
| Type | 1 byte | `B` (uint8) | Packet type identifier | `2` (`MSG`) |
| Flags | 1 byte | `B` (uint8) | Bitmask control flags | `1` (`FLAG_ENCRYPTED`) |
| Sequence number | 4 bytes | `I` (uint32) | Monotonically increasing packet sequence number | `1001` |
| Acknowledgment number | 4 bytes | `I` (uint32) | Sequence number being acknowledged | `1001` |
| Message ID | 16 bytes | `16s` | Unique UUID v4 for the message or file transfer session | `uuid4().bytes` |
| Chunk index | 2 bytes | `H` (uint16) | Current chunk sequence (0-indexed) | `0` |
| Total chunks | 2 bytes | `H` (uint16) | Total number of chunks in the transfer | `1` |
| Payload length | 4 bytes | `I` (uint32) | Size of payload data following the header (bytes) | `64` |

### 3.2 Packet Types

| Value | Name | Description |
| :---: | --- | --- |
| 1 | `HEARTBEAT` | Client periodic presence signal (`{"username": "..."}`) |
| 2 | `MSG` | Real-time chat message (unicast / broadcast) |
| 3 | `FILE_META` | File transfer metadata (filename, size, checksum) |
| 4 | `FILE_CHUNK` | Raw binary chunk of a file |
| 5 | `ACK` | Acknowledgment packet carrying acknowledged sequence number |
| 6 | `USER_LIST` | Broadcast list of currently active users |

### 3.3 Control Flags

| Bit | Name | Value | Description |
| :---: | --- | :---: | --- |
| 0 | `FLAG_ENCRYPTED` | `0x01` | Payload is encrypted with Fernet |
| 1 | `FLAG_END` | `0x02` | Marks final packet / end-of-transmission chunk |

Payloads with `FLAG_ENCRYPTED` set are encrypted using Fernet symmetric encryption before transmission.

Reliable UDP uses sequence numbers, ACKs, retransmission timeouts, and a sliding window.

### 3.4 UDP Message Payloads

The payload that follows the 37-byte header is a UTF-8 encoded JSON object, unless the packet type says otherwise. `encode_packet()` in `protocol.py` encrypts every non-empty payload with Fernet by default and sets `FLAG_ENCRYPTED`; `decode_packet()` decrypts it automatically. The JSON examples below show the payload **before** encryption. (If the team decides to send Week 3 packets in plaintext for easier debugging, call `encode_packet(..., encrypt=False)` and update this paragraph.)

> **Two different `USER_LIST` messages:**
> - **TCP control message** (`{"type": "USER_LIST"}` request, `{"type": "USER_LIST", "status": "OK", "users": [...]}` response): the client asks for the list on demand. Each user is `{"username", "ip", "port"}`.
> - **UDP packet type 6**: pushed by the server to all clients when the set of online users changes. Payload is `{"users": ["alice", "bob"]}` (usernames only).

#### MSG (type = 2), Client <-> Server

Used for both broadcast and unicast chat messages.

| Field  | Type   | Required | Description                                                  | Example        |
| ------ | ------ | :------: | ------------------------------------------------------------ | -------------- |
| `to`   | string |   Yes    | `"*"` for broadcast, otherwise the recipient's username       | `"*"`, `"bob"` |
| `from` | string |   Yes    | Username of the sender                                       | `"alice"`      |
| `text` | string |   Yes    | Message content                                              | `"hello"`      |
| `time` | number |   Yes    | Unix timestamp in seconds, set by the sender                 | `1761300000`   |

```json
{"to": "*", "from": "alice", "text": "hello everyone", "time": 1761300000}
```

```json
{"to": "bob", "from": "alice", "text": "hi bob", "time": 1761300005}
```

Routing rules:
- `to = "*"`: the server forwards the message to every online user (see the server plan for whether the sender is excluded).
- `to = <username>`: the server forwards the message only to that user.

#### HEARTBEAT (type = 1), Client -> Server

Sent periodically (every 2 seconds) by an authenticated client. The server updates the user's `last_seen` time. If no heartbeat arrives for 10 seconds, the server removes the user.

| Field      | Type   | Required | Description                         | Example   |
| ---------- | ------ | :------: | ----------------------------------- | --------- |
| `username` | string |   Yes    | Username of the client that is alive | `"alice"` |

```json
{"username": "alice"}
```

#### USER_LIST (type = 6), Server -> Client

Sent by the server whenever the set of online users changes (a user logs in, leaves, or is removed by timeout).

| Field   | Type            | Required | Description                      | Example             |
| ------- | --------------- | :------: | -------------------------------- | ------------------- |
| `users` | array of string |   Yes    | Usernames of all online users    | `["alice", "bob"]`  |

```json
{"users": ["alice", "bob"]}
```

### 3.5 Header Field Values per Packet Type

| Packet type | Flags (set by `encode_packet`) | Sequence number           | Ack number | Message ID        | Chunk index | Total chunks |
| ----------- | :---: | ------------------------- | :--------: | ----------------- | :---------: | :----------: |
| `HEARTBEAT` | `FLAG_ENCRYPTED` | increasing per sender     | `0`        | 16 zero bytes     | `0`         | `1`          |
| `MSG`       | `FLAG_ENCRYPTED` | increasing per sender     | `0`        | `uuid4().bytes`   | `0`         | `1`          |
| `USER_LIST` | `FLAG_ENCRYPTED` | increasing per server     | `0`        | 16 zero bytes     | `0`         | `1`          |

Notes:
- The sequence number starts at 1 and increases by 1 for every datagram a sender transmits. It is filled in now so that Week 4 only needs to add ACK handling and retransmission.
- The ack number stays `0` until the reliability layer is added.
- Every UDP datagram carries exactly one packet. No length prefix is used on UDP, because the datagram boundary already marks the end of the packet.
- Keep the whole datagram (header + payload) below about 1400 bytes to avoid IP fragmentation. Because Fernet encryption makes the payload roughly 4/3 of its plaintext size plus some tens of bytes, the plaintext JSON must be well under 1000 bytes. The helpers enforce `MAX_UDP_SIZE = 1400` bytes and `MAX_TEXT_LEN = 250` characters (250 Vietnamese characters with diacritics, 3 bytes each in UTF-8, still fit after encryption). Payload JSON is written as UTF-8, not `\uXXXX` escapes, otherwise accented text would be about twice as large.

### 3.6 Packet Helper Functions (`protocol.py`)

Helpers for building and parsing UDP packets. They are thin wrappers around the existing `Packet`, `encode_packet()` and `decode_packet()` in `protocol.py`. The server and the client must use these helpers instead of calling `struct` directly.

| Function                                  | Description                                                                  |
| ----------------------------------------- | ---------------------------------------------------------------------------- |
| `pack_udp(ptype, payload, seq, ...)`      | Builds a full datagram: 37-byte header + payload bytes. Returns `bytes`.     |
| `unpack_udp(data)`                        | Parses a datagram into a header and a payload. Raises an error if the magic number, version, or payload length is invalid. |
| `pack_msg(to, sender, text, seq)`         | Convenience wrapper that builds a `MSG` packet (adds `time` automatically).  |
| `pack_heartbeat(username, seq)`           | Convenience wrapper that builds a `HEARTBEAT` packet.                        |
| `pack_user_list(users, seq)`              | Convenience wrapper that builds a `USER_LIST` packet.                        |

Error handling: a datagram that is too short, has the wrong magic number, or whose declared payload length does not match the received data must be rejected with an error (and ignored by the receiver), never silently accepted.

## 4. Message Types & Protocol Vocabulary

This section defines the core application message types, protocol vocabulary, and connection state transitions for the client-server architecture.

To ensure clear communication between clients and the server, all application messages are categorized into a standardized protocol vocabulary.

### Message Types Table

| Value / Code | Name | Direction | Transport | Description |
| :---: | --- | :---: | :---: | --- |
| - | `HELLO` | Client $\leftrightarrow$ Server | TCP / UDP | Initial handshake signal to establish connection readiness. |
| 5 | `ACK` | Client $\leftrightarrow$ Server | UDP | Acknowledgment packet carrying confirmation of received segments. |
| - | `AUTH` | Client $\rightarrow$ Server | TCP / UDP | Authentication payload carrying username and credentials for registration. |
| 2 | `MSG` | Client $\leftrightarrow$ Server | UDP / TCP | Real-time chat message payload (broadcast or unicast). |
| 1 | `HEARTBEAT` | Client $\rightarrow$ Server | UDP | Periodic liveness signal to maintain active session presence. |
| 3 | `FILE_META` | Client $\leftrightarrow$ Server | TCP / UDP | File transfer metadata including filename, size, and total chunks. |
| 4 | `FILE_CHUNK` | Client $\leftrightarrow$ Server | UDP | Raw binary segment / chunk of an active file transfer. |
|      6       | `USER_LIST`  |   Server $\rightarrow$ Client   |    UDP    | Broadcast list of currently active users, sent when the user set changes.  |
| - | `BYE` | Client $\leftrightarrow$ Server | TCP / UDP | Graceful disconnection notice indicating session teardown. |

## 5. Connection State Diagram

The connection lifecycle transitions through distinct states managed by event triggers on both the client and server sides.

```mermaid
stateDiagram-v2
    [*] --> CLOSED
    CLOSED --> CONNECTING : Initiate connection / Socket open
    CONNECTING --> ESTABLISHED : Receive HELLO / Auth success
    ESTABLISHED --> CLOSING : Receive BYE / Disconnect request
    ESTABLISHED --> CLOSING : Connection timeout / Network error
    CLOSING --> CLOSED : Cleanup resources & close socket
    ESTABLISHED --> CONNECTING : Reconnect / Session retry
```

## 6. Authentication Protocol (AUTH, AUTH_OK, AUTH_FAIL)

All authentication messages are exchanged over the TCP control channel (port 9000) using length-prefix framing with JSON payloads.

### 6.1 AUTH Request (Client -> Server)
Sent by the client immediately after the TCP handshake to authenticate and register its active session.

| Field | Type | Required | Description | Example |
| :--- | :---: | :---: | :--- | :--- |
| `type` | string | Yes | Message identifier, must be `"AUTH"` | `"AUTH"` |
| `username` | string | Yes | The user account name | `"alice"` |
| `password` | string | Yes | The user account password | `"pass123"` |
| `udp_port` | int | No | The UDP port the client listens on to receive data-channel packets (not the server's port 9001). The server combines it with the client's IP address from `accept()` to know where to send UDP packets. | `50001` |

**Example payload:**
```json
{
  "type": "AUTH",
  "username": "alice",
  "password": "pass123",
  "udp_port": 50001
}
```

### 6.2 AUTH_OK Response (Server -> Client)
Sent by the server when credentials are valid and the user is not currently logged in.

| Field       | Type            | Required | Description                                                        | Example                                  |
| ----------- | --------------- | :------: | ------------------------------------------------------------------ | ---------------------------------------- |
| `type`      | string          |   Yes    | Must be `"AUTH_OK"`                                                | `"AUTH_OK"`                              |
| `status`    | string          |   Yes    | Always `"OK"`                                                      | `"OK"`                                   |
| `user`      | string          |   Yes    | The authenticated username                                         | `"alice"`                                |
| `users`     | array of object |   Yes    | Currently online users, each `{"username", "ip", "port"}`          | `[{"username":"bob","ip":"127.0.0.1","port":52134}]` |
| `user_list` | array of object |    No    | Duplicate of `users` kept for compatibility                        | same as `users`                          |

**Example payload:**
```json
{
  "type": "AUTH_OK",
  "status": "OK",
  "user": "alice",
  "users": [{"username": "bob", "ip": "127.0.0.1", "port": 52134}],
  "user_list": [{"username": "bob", "ip": "127.0.0.1", "port": 52134}]
}
```

### 6.3 AUTH_FAIL Response (Server -> Client)
Sent by the server when authentication is rejected.

| Field     | Type   | Required | Description                                                                    | Example                                  |
| --------- | ------ | :------: | ------------------------------------------------------------------------------ | ---------------------------------------- |
| `type`    | string |   Yes    | Must be `"AUTH_FAIL"`                                                          | `"AUTH_FAIL"`                            |
| `status`  | string |   Yes    | Always `"ERROR"`                                                               | `"ERROR"`                                |
| `message` | string |   Yes    | Human-readable cause: invalid username/password, or user already logged in     | `"User 'alice' already logged in"`       |

**Example payload:**
```json
{
  "type": "AUTH_FAIL",
  "status": "ERROR",
  "message": "Authentication failed: invalid username or password"
}
```

### 6.4 Authentication Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Server
    participant Registry as UserRegistry

    Client->>Server: TCP connect
    Server->>Client: {"type": "ACK", "status": "OK"} (Handshake)
    Client->>Server: {"type": "AUTH", "username": "alice", "password": "pass123"}
    
    Server->>Registry: authenticate("alice", "pass123")
    alt Credentials Invalid
        Server->>Client: {"type": "AUTH_FAIL", "status": "ERROR", "message": "Authentication failed: invalid username or password"}
    else Credentials Valid
        Server->>Registry: register("alice", ip, port, sock)
        alt Already Logged In
            Server->>Client: {"type": "AUTH_FAIL", "status": "ERROR", "message": "User 'alice' already logged in"}
        else Register Success
            Server->>Client: {"type": "AUTH_OK", "status": "OK", "user": "alice", "users": [...]}
        end
    end
```

## 7. Message Flow

```mermaid
sequenceDiagram
    ClientA->>Server: UDP MSG(seq)
    Server-->>ClientA: ACK(seq)
    Server->>ClientB: UDP MSG(new seq)
    ClientB-->>Server: ACK(new seq)
```

> **Note (Week 3):** In this version, `MSG` and `HEARTBEAT` are sent over plain UDP without ACK or retransmission, so a lost datagram is not recovered. The ACK and retransmission shown above will be added in Week 4 together with the sliding window. The sequence diagrams for broadcast, unicast, and heartbeat are added by the testing/diagram owner.

## 8. File Flow

```mermaid
sequenceDiagram
    ClientA->>Server: FILE_META
    Server-->>ClientA: ACK
    loop sliding window
        ClientA->>Server: FILE_CHUNK
        Server-->>ClientA: ACK
    end
    Server->>ClientB: FILE_CHUNK
    ClientB-->>Server: ACK
```

## 9. Connection Sequence Diagram (Handshake -> Auth -> Disconnect)

Sơ đồ trình tự (Sequence Diagram) mô tả toàn bộ vòng đời kết nối từ lúc Client thiết lập TCP, gửi bản tin `HELLO`, thực hiện xác thực `AUTH`, duy trì phiên và ngắt kết nối an toàn (`BYE` hoặc ngắt đột ngột):

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Server
    participant UserRegistry

    Note over Client,Server: Phase 1: TCP Connection & HELLO Handshake
    Client->>Server: TCP SYN (Establish TCP Connection)
    Server-->>Client: TCP SYN-ACK / ACK
    Note right of Server: Connection accepted (CLOSED -> CONNECTING)

    Client->>Server: HELLO {"type": "HELLO", "client": "alice"}
    Note right of Server: Server processes HELLO
    Server-->>Client: ACK {"type": "ACK", "status": "OK"}
    Note right of Client: Handshake successful (ESTABLISHED)

    Note over Client,Server: Phase 2: Authentication (AUTH)
    Client->>Server: AUTH {"type": "AUTH", "username": "alice", "password": "pass123"}
    Server->>UserRegistry: authenticate("alice", "pass123")
    UserRegistry-->>Server: True (Valid credentials)
    Server->>UserRegistry: register("alice", IP, TCP_port, sock)
    UserRegistry-->>Server: Success ("OK")
    Server-->>Client: AUTH_OK {"type": "AUTH_OK", "status": "OK", "user": "alice", "users": [...]}
    Note right of Server: Active Users table updated (IP & TCP port tracked)

    Note over Client,Server: Phase 3: Active Session & Disconnection
    rect rgb(240, 240, 240)
        Note right of Client: Client active, chatting / heartbeat / files
    end

    alt Graceful Disconnect
        Client->>Server: BYE {"type": "BYE", "client": "alice"}
        Note right of Server: Client closes connection gracefully (CLOSING)
        Server->>UserRegistry: unregister("alice")
        UserRegistry-->>Server: Removed from active users
        Server-->>Client: Close TCP socket
    else Abrupt Disconnect (Network drop / Crash / Ctrl+C)
        Client x-) Server: Connection lost (ConnectionResetError)
        Note right of Server: Server detects socket error in recv loop
        Server->>UserRegistry: unregister("alice") (Automatic cleanup)
        Note right of Server: State cleaned up, Active Users updated
    end
```

## 10. Sequence Diagram cho Broadcast, Unicast và Heartbeat
a) Sequence Diagram cho Broadcast chat
```mermaid
sequenceDiagram
    autonumber
    participant Alice as Client Alice
    participant Server as UDP Server (Port 9001)
    participant Bob as Client Bob
    participant Charlie as Client Charlie

    Alice->>Server: UDP Datagram (ptype=MSG, to="*", text="Hello")
    Note over Server: Server nhận & kiểm tra quyền
    Server->>Bob: Forward UDP Datagram (from="alice", text="Hello")
    Server->>Charlie: Forward UDP Datagram (from="alice", text="Hello")
```
b) Sequence Diagram cho Unicast chat
```mermaid
sequenceDiagram
    autonumber
    participant Alice as Client Alice
    participant Server as UDP Server (Port 9001)
    participant Bob as Client Bob
    participant Charlie as Client Charlie

    Alice->>Server: UDP Datagram (ptype=MSG, to="bob", text="Hi Bob")
    Note over Server: Server tra cứu địa chỉ IP:Port của Bob
    Server->>Bob: Forward UDP Datagram (from="alice", text="Hi Bob")
    Note over Charlie: Charlie KHÔNG nhận được bản tin này
```
c) Sequence Diagram cho Heartbeat 
```mermaid
sequenceDiagram
    autonumber
    participant Client as Client Alice
    participant Server as UDP Server
    participant CleanThread as Cleanup Loop (Daemon)

    loop Định kỳ mỗi 2 giây
        Client->>Server: UDP Datagram (ptype=HEARTBEAT, username="alice")
        Note over Server: Cập nhật last_seen["alice"] = now()
    end

    Note over Client: Client đột ngột sập / ngắt mạng
    Note over Server: Ngừng nhận HEARTBEAT từ Alice

    loop Quét định kỳ
        CleanThread->>Server: Kiểm tra timeout (now - last_seen > 10s)
        Note over Server: Phát hiện Alice quá 10s không heartbeat
        Server->>Server: registry.unregister("alice")
        Server->>All: Broadcast USER_LIST cập nhật (Alice offline)
    end
```
