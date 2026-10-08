# Architecture

```mermaid
flowchart LR
    A[Browser UI A] --> CA[Client A]
    B[Browser UI B] --> CB[Client B]
    CA -->|TCP auth and control| S[Server]
    CB -->|TCP auth and control| S
    CA -->|UDP streaming| S
    CB -->|UDP streaming| S
    S -->|UDP routing| CA
    S -->|UDP routing| CB
```

The application separates the browser UI, client network logic, server state, protocol encoding and cryptography.

The browser communicates only with its local client process. Client to server networking uses raw TCP and UDP sockets.
