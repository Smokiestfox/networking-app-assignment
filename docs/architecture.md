# System Architecture – Networking App Assignment

> **Scope:** Week 2 – State Management & Concurrency Implementation (Rubric 3 & 4)

---

## 1. Three-Layer Architecture

The system follows a strict three-tier separation of concerns:

```mermaid
graph TD
    subgraph L3["Layer 3 – Presentation"]
        UI["Web UI (static/index.html - Week 5)"]
        CLI["CLI Interactive / Logging (client.py)"]
        SIM["Demo Simulator (simulate_demo.py)"]
    end

    subgraph L2["Layer 2 – Application & State"]
        CLIENT["client.py (Session State: IDLE -> LOGGED_IN)"]
        SERVER["server.py (Multi-threaded TCP Server)"]
        REG["UserRegistry (Thread-safe Active Users Table)"]
        AUTH_DB[("users.json (Credentials Database)")]
    end

    subgraph L1["Layer 1 – Network & Protocol"]
        PROTO["protocol.py (Frame Header !I + Packet Struct)"]
        CFG["config.py (SERVER_HOST, TCP_PORT, UDP_PORT)"]
    end

    CLI --> CLIENT
    SIM --> CLIENT
    CLIENT <-->|TCP raw socket / Length-Prefix Framing| SERVER
    SERVER <--> REG
    REG <--> AUTH_DB
    CLIENT --> PROTO
    SERVER --> PROTO
    PROTO --> CFG
```

---

## 2. Server Concurrency Architecture (Multi-threading)

To handle 10–20+ simultaneous clients without blocking the main event loop (Rubric 4), the Server employs a **thread-per-client model**:

```mermaid
flowchart TD
    MainThread["Main Thread (CollaborationServer.run)\nsock.accept() loop"]
    Client1["Client 1\n(alice)"] -->|TCP Connect| MainThread
    Client2["Client 2\n(bob)"] -->|TCP Connect| MainThread
    ClientN["Client N\n(user15)"] -->|TCP Connect| MainThread

    MainThread -->|spawn thread daemon=True| T1["Worker Thread 1\nhandle_client(conn1, addr1)"]
    MainThread -->|spawn thread daemon=True| T2["Worker Thread 2\nhandle_client(conn2, addr2)"]
    MainThread -->|spawn thread daemon=True| TN["Worker Thread N\nhandle_client(connN, addrN)"]

    T1 <-->|atomic with lock| Registry[("UserRegistry\nactive_users: {user: {ip, port, conn}}")]
    T2 <-->|atomic with lock| Registry
    TN <-->|atomic with lock| Registry
```

- **Thread Isolation:** Each client connection is handled by a dedicated worker thread (`threading.Thread(target=self.handle_client, daemon=True)`). A slow or abruptly disconnected client does not block other clients or the server's primary `accept()` listener.
- **Thread Safety:** All read, write, and mutation operations on the active user registry are synchronized via `threading.Lock()`, completely preventing race conditions during concurrent client logins or logouts.

---

## 3. Sequence Diagram – Week 2 Authentication & Session Lifecycle

Connection, authentication, and session management lifecycle between Client and Server (Rubric 3 & 4):

```mermaid
sequenceDiagram
    autonumber
    participant C as client.py (Chinh)
    participant S as server.py (Binh)
    participant R as UserRegistry

    Note over C,S: 1. TCP 3-Way Handshake
    C->>S: TCP connect (port 9000)
    S-->>C: Connected

    Note over C,S: 2. Initial Handshake (Week 1 Compatibility)
    C->>S: HELLO {"type": "HELLO", "client": "alice"}
    S-->>C: ACK {"type": "ACK", "status": "OK"}

    Note over C,S: 3. Authentication & State Tracking
    C->>S: AUTH {"type": "AUTH", "username": "alice", "password": "..."}
    S->>R: authenticate(username, password)
    R-->>S: OK
    S->>R: register(username, ip, port, conn)
    R-->>S: True (Saved in Active Users)
    S-->>C: AUTH_OK {"type": "AUTH_OK", "user": "alice", "users": [...]}

    Note over C,S: 4. Active Session (State: LOGGED_IN)
    opt User queries peers
        C->>S: USER_LIST {"type": "USER_LIST"}
        S-->>C: USER_LIST {"users": [{"username": "alice", "ip": "...", "port": ...}]}
    end

    Note over C,S: 5. Graceful Disconnection
    C->>S: BYE {"type": "BYE", "username": "alice"}
    S->>R: unregister("alice")
    S-->>C: ACK {"type": "ACK", "status": "BYE"}
    C->>S: Close TCP socket
```

---

## 4. Client State Machine

Client session transitions based on network events:

```mermaid
stateDiagram-v2
    [*] --> DISCONNECTED
    DISCONNECTED --> CONNECTING : sock.connect()
    CONNECTING --> HANDSHAKING : TCP Connected
    HANDSHAKING --> AUTHENTICATING : Receive ACK for HELLO
    AUTHENTICATING --> LOGGED_IN : Receive AUTH_OK
    AUTHENTICATING --> DISCONNECTED : Receive AUTH_FAIL / Wrong credentials
    LOGGED_IN --> LOGGED_IN : Query USER_LIST / Idle Keep-Alive
    LOGGED_IN --> DISCONNECTING : Send BYE / KeyboardInterrupt
    DISCONNECTING --> DISCONNECTED : Receive ACK / Socket closed
    DISCONNECTED --> [*]
```
