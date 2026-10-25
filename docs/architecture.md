# System Architecture – Networking App Assignment

> **Scope:** Week 3 – UDP Messaging, Heartbeat & Liveness (Rubric 5), built on Week 1-2 (handshake, authentication, concurrency).
>
> **Status note:** The Server UDP Engine (routing + liveness monitor) is **not yet merged into `server.py`**. Until it is, `server_demo.py` runs the real TCP server plus a temporary UDP engine (`DemoUdpEngine`) so that `client.py` can be tested end to end. Sections marked **[Planned in server.py]** describe the target design; the demo already implements it in one process.

---

## 1. Three-Layer Architecture

The system keeps network logic, state/data processing, and the user interface separate:

```mermaid
graph TD
    subgraph L3["Layer 3 – Presentation"]
        CLI["CLI Chat (client.py): /all /to /list /quit"]
        SIM["Demo Simulator (simulate_demo.py)"]
        UI["Web UI (Week 5)"]
    end

    subgraph L2["Layer 2 – Application & State"]
        CLIENT["client.py (session state + 3 threads)"]
        SERVER["server.py: TCP control server (thread-per-client)"]
        UDPENG["Server UDP Engine: receiver, router, cleanup loop<br/>[Planned in server.py - temporary: server_demo.py]"]
        REG["UserRegistry (registry.py, thread-safe active users)"]
        AUTH_DB[("users.json")]
    end

    subgraph L1["Layer 1 – Network & Protocol"]
        PROTO["protocol.py: TCP framing + UDP helpers (build_udp / parse_udp, pack_*, SeqCounter)"]
        CFG["config.py (ports, HEARTBEAT_INTERVAL, CLIENT_TIMEOUT, Fernet key)"]
    end

    CLI --> CLIENT
    SIM --> CLIENT
    UI -.-> CLIENT
    CLIENT <-->|"TCP 9000: HELLO / AUTH / USER_LIST / BYE"| SERVER
    CLIENT <-->|"UDP 9001: HEARTBEAT / MSG / USER_LIST"| UDPENG
    SERVER <--> REG
    UDPENG <--> REG
    REG <--> AUTH_DB
    CLIENT --> PROTO
    SERVER --> PROTO
    UDPENG --> PROTO
    PROTO --> CFG
```

**Dual-channel design**

| Channel     | Port | Purpose                                                        | Framing                                                         |
| ----------- | :--: | -------------------------------------------------------------- | --------------------------------------------------------------- |
| TCP control | 9000 | Handshake, login, logout, on-demand user list                  | 4-byte length prefix + JSON                                     |
| UDP data    | 9001 | Heartbeat, chat messages (broadcast / unicast), user-list push | One packet per datagram: 37-byte header + Fernet-encrypted JSON |

The TCP session is the source of truth for **who is logged in**. The UDP channel only carries real-time traffic for users who are already active in the registry.

---

## 2. Server Concurrency Architecture

The server uses a thread-per-client model for TCP and two dedicated daemon threads for UDP:

```mermaid
flowchart TD
    Main["Main Thread<br/>TCP accept() loop (port 9000)"]
    Main -->|"spawn daemon thread"| T1["TCP worker 1<br/>handle_client(alice)"]
    Main -->|"spawn daemon thread"| T2["TCP worker 2<br/>handle_client(bob)"]
    Main -->|"spawn daemon thread"| TN["TCP worker N<br/>handle_client(...)"]

    RX["udp-recv thread<br/>recvfrom() on port 9001<br/>parse_udp -> HEARTBEAT / MSG"]
    CL["udp-cleanup thread<br/>cleanup_loop() every 1 s"]

    T1 <-->|"registry.lock"| Reg[("UserRegistry<br/>active_users")]
    T2 <-->|"registry.lock"| Reg
    TN <-->|"registry.lock"| Reg
    RX <-->|"is_active()"| Reg
    CL <-->|"get_active_users() / unregister()"| Reg

    RX <-->|"udp lock"| UState[("UDP state<br/>addrs: user -> (ip, udp_port)<br/>last_seen: user -> time")]
    CL <-->|"udp lock"| UState
```

- **Thread isolation:** a slow or abruptly closed TCP client only affects its own worker thread.
- **UDP never blocks TCP:** the UDP receive loop uses a 0.5 s socket timeout so it can stop cleanly, and handles `ConnectionResetError` (Windows reports ICMP "port unreachable" this way) by continuing.
- **Two locks, two responsibilities:** `registry.lock` protects the active-user table; a separate lock protects the UDP address and `last_seen` maps. Threads never hold both at the same time, which avoids deadlocks.

---

## 3. Server UDP Engine – Routing & Liveness Monitor **[Planned in server.py]**

### 3.1 Responsibilities

| Component                                     | Behaviour                                                                                                                                                                                                                                            |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| UDP socket                                    | Bind `SERVER_HOST:UDP_PORT` (9001), run in parallel with the TCP listener inside `server.py`.                                                                                                                                                    |
| Receive loop                                  | `recvfrom()` -> `parse_udp()`. A datagram that fails validation is logged and ignored, never crashes the server.                                                                                                                                 |
| `HEARTBEAT` handler                         | Ignore users who are not logged in over TCP (`registry.is_active`). Otherwise store the **source address of the datagram** as the user's UDP address and refresh `last_seen`. The first heartbeat of a user triggers a `USER_LIST` push. |
| `MSG` handler / `forward(sender, packet)` | Identify the sender from the datagram's source address (the `from` field sent by the client is overwritten with the registered username). Route by `to`.                                                                                         |
| `cleanup_loop`                              | Every second: remove users whose TCP session has ended, and users with no heartbeat for `CLIENT_TIMEOUT` (10 s). Broadcast the new `USER_LIST` after every removal.                                                                              |

### 3.2 Routing rules

| `to` value                                             | Recipients                                 | Not delivered to                    |
| -------------------------------------------------------- | ------------------------------------------ | ----------------------------------- |
| `"*"` (broadcast)                                      | Every user with a known UDP address        | The sender                          |
| `"<username>"` (unicast)                               | Only that user                             | Everyone else, including the sender |
| Unknown / offline user, or sender address not registered | Nobody; the datagram is dropped and logged | –                                  |

### 3.3 Heartbeat timeout

When a user is alive over TCP but sends no heartbeat for 10 s, the engine unregisters the user, closes the user's TCP connection (so the TCP worker thread finishes), removes the UDP state, and pushes a new `USER_LIST` to everyone left.

---

## 4. Client Architecture

`client.py` runs the TCP control session on the main thread and two UDP background threads:

```mermaid
flowchart LR
    subgraph Client["client.py (run_client)"]
        Main["Main thread<br/>TCP: HELLO -> AUTH -> BYE<br/>CLI input loop"]
        HB["heartbeat thread<br/>sends HEARTBEAT every 2 s"]
        LS["udp-listener thread<br/>prints MSG, join / leave notices"]
        US(("UDP socket<br/>ephemeral port"))
    end

    Main -->|"/all, /to: pack_msg + sendto"| US
    HB -->|"pack_heartbeat + sendto"| US
    US -->|"recvfrom + parse_udp"| LS
    US <-->|"UDP 9001"| S["Server UDP Engine"]
    Main <-->|"TCP 9000"| T["Server TCP"]
```

| CLI command           | Action                                    | Channel           |
| --------------------- | ----------------------------------------- | ----------------- |
| `/all <text>`       | Broadcast (`to = "*"`)                  | UDP               |
| `/to <user> <text>` | Private message (`to = <user>`)         | UDP               |
| `/list`             | Request the user list (with IP and port)  | TCP `USER_LIST` |
| `/quit`             | Stop threads, send `BYE`, close sockets | TCP               |

- The client binds an ephemeral UDP port and sends one `HEARTBEAT` immediately after login, before any chat message, so the server learns its UDP address.
- Incoming `USER_LIST` datagrams contain only usernames; the listener compares them with the last known set and prints `[+] name is now online` / `[-] name went offline`.
- Broadcast needs the explicit `/all` command, so a typo cannot message everyone.

---

## 5. Sequence Diagram – Session Lifecycle (TCP + UDP)

```mermaid
sequenceDiagram
    autonumber
    participant C as client.py
    participant S as server.py TCP 
    participant U as Server UDP Engine 
    participant R as UserRegistry 

    Note over C,S: 1. TCP handshake and authentication (port 9000)
    C->>S: HELLO
    S-->>C: ACK
    C->>S: AUTH {username, password}
    S->>R: authenticate() then register(username, ip, port, conn)
    S-->>C: AUTH_OK {user, users}

    Note over C,U: 2. UDP registration (port 9001)
    C->>U: HEARTBEAT {username}
    U->>R: is_active(username)
    U->>U: store source address, last_seen = now
    U-->>C: USER_LIST {users} (pushed to everyone)

    Note over C,U: 3. Chat and liveness
    loop every 2 s
        C->>U: HEARTBEAT
    end
    C->>U: MSG {to: "*" or "bob", text}
    U-->>C: MSG delivered to recipients only (see section 3.2)

    alt Graceful exit
        C->>S: BYE (TCP)
        S->>R: unregister(username)
        U->>U: cleanup_loop sees TCP session ended
        U-->>C: USER_LIST pushed to the remaining users
    else Crash / window closed
        Note over U: no HEARTBEAT for 10 s
        U->>R: unregister(username), close TCP connection
        U-->>C: USER_LIST pushed to the remaining users
    end
```

Detailed Broadcast, Unicast and Heartbeat sequence diagrams are maintained in `docs/protocol.md` (Quynh).

---

## 6. Client State Machine

```mermaid
stateDiagram-v2
    [*] --> DISCONNECTED
    DISCONNECTED --> CONNECTING : sock.connect()
    CONNECTING --> HANDSHAKING : TCP connected
    HANDSHAKING --> AUTHENTICATING : ACK for HELLO
    AUTHENTICATING --> LOGGED_IN : AUTH_OK, start UDP threads
    AUTHENTICATING --> DISCONNECTED : AUTH_FAIL
    LOGGED_IN --> LOGGED_IN : send / receive MSG, HEARTBEAT, USER_LIST
    LOGGED_IN --> DISCONNECTING : /quit, Ctrl+C
    LOGGED_IN --> DISCONNECTED : server closes TCP (heartbeat timeout)
    DISCONNECTING --> DISCONNECTED : BYE acknowledged, sockets closed
    DISCONNECTED --> [*]
```
