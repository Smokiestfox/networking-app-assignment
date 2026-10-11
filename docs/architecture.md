# System Architecture – Networking App Assignment

> **Scope:** Week 1 layers may be noted but not yet implemented.

---

## 1. Three-Layer Architecture

```mermaid
graph TD
    subgraph L3["Layer 3 – Presentation"]
        UI["Web UI\nstatic/index.html"]
        CLI["CLI / Logging\n"]
    end

    subgraph L2["Layer 2 – Application"]
        CLIENT["client.py"]
        SERVER["server.py"]
    end

    subgraph L1["Layer 1 – Protocol"]
        PROTO["protocol.py"]
        CFG["config.py"]
    end

    UI -->|HTTP localhost| CLIENT
    CLI --> CLIENT
    CLIENT <-->|TCP raw socket| SERVER
    CLIENT --> PROTO
    SERVER --> PROTO
    PROTO --> CFG
```

---

## 2. Sequence Diagram – W1 Handshake

```mermaid
sequenceDiagram
    participant C as client.py
    participant S as server.py

    C->>S: TCP connect (3-way handshake)
    Note over C,S: TCP connection established

    C->>S: HELLO (ptype=0, seq=1, payload="HELLO")
    S-->>C: ACK   (ptype=5, ack=1)

    C->>S: BYE   (ptype=7, seq=2, payload="BYE")
    C->>S: TCP close
```

---


## 3. Data Flow

```mermaid
flowchart LR
    Browser["Browser / CLI"] -->|"calls run_client()"| CP["client.py"]
    CP -->|"encode_packet()\nsend_frame()"| TCP["TCP Socket"]
    TCP -->|"recv_frame()\ndecode_packet()"| SP["server.py"]
    SP -->|"encode_packet()\nsend_frame()"| TCP
    TCP -->|"recv_frame()\ndecode_packet()"| CP

    CP <-->|import| PL["protocol.py"]
    SP <-->|import| PL
    PL <-->|import| CFG["config.py"]
```

---
