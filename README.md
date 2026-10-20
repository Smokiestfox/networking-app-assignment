# Networking App Assignment

Ứng dụng cộng tác client-server tự host cho mạng LAN/VPN: đăng nhập, xem danh sách user online, chat chung và chat riêng, truyền file tin cậy, có mã hóa payload. Toàn bộ phần mạng viết bằng raw socket (không dùng Flask, Django hay WebSocket).

Môn: Network Application Development, Assignment 1.

## Tính năng dự kiến

- Đăng nhập, server theo dõi user (tên, IP, port) và phát danh sách user online
- Chat chung (broadcast) và chat riêng (unicast) qua server
- Truyền file tin cậy trên UDP: chia chunk, sliding window, chỉ gửi lại gói thiếu, kiểm tra SHA-256
- Heartbeat, timeout, xử lý client thoát đột ngột
- Mã hóa payload (AES-GCM)
- Giao diện web (HTML + JavaScript thuần): tin nhắn, progress bar, danh sách user

## Cấu trúc thư mục dự kiến

```
.
├── client.py            # chạy client
├── server.py            # chạy server
├── protocol.py          # packet format, message types
├── config.py            # host, port, timeout, kích thước window/chunk
├── users.json           # dữ liệu tài khoản
├── requirements.txt
├── static/
│   └── index.html       # giao diện web
├── tests/
└── docs/
    ├── architecture.md  # kiến trúc hệ thống
    └── protocol.md      # đặc tả giao thức
```

## Hướng dẫn chạy và Demo (Week 2: State & Concurrency)

### 1. Chạy toàn bộ Unit Tests tự động

Kiểm tra toàn bộ giao thức (protocol), xác thực (authentication) và khả năng xử lý đa luồng (multi-threading):

```bash
python -m pytest tests -v
```

*(Hoặc chạy riêng từng bài test: `python -m pytest tests/test_protocol.py` hoặc `python -m pytest tests/test_server.py`)*

---

### 2. Demo tương tác Client – Server (Authentication & Session)

**Bước 1: Khởi chạy Server (Terminal 1)**

```bash
python server.py
```

Server sẽ lắng nghe trên cổng `9000` (TCP), quản lý đa luồng và hiển thị bảng Active Users thời gian thực.

**Bước 2: Khởi chạy Client (Terminal 2)**

```bash
# Đăng nhập với tài khoản có trong users.json:
python client.py alice pass123

# Hoặc chạy tương tác để nhập từ bàn phím:
python client.py
```

- **Luồng xử lý:** Client tự động thực hiện handshake `HELLO -> ACK`, gửi xác thực `AUTH -> AUTH_OK`, và in bảng danh sách user đang online.
- **Lệnh tương tác:**
  - Gõ `list` để làm mới danh sách user online.
  - Gõ `quit` hoặc nhấn `Ctrl+C` để gửi `BYE` và kết thúc phiên an toàn.

---

### 3. Demo mô phỏng tải 10–15 Client đồng thời (Concurrency Load Test)

Dùng để kiểm chứng tiêu chí **Rubric 3 (State Tracking)** & **Rubric 4 (Multi-threaded Concurrency)**:

**Bước 1:** Đảm bảo Server đang chạy ở Terminal 1 (`python server.py`).

**Bước 2:** Mở Terminal 2 và kích hoạt script mô phỏng:

```bash
python simulate_demo.py --count 12 --duration 10
```

- Script sẽ khởi tạo đồng thời 12 luồng client độc lập (`user01` đến `user12`).
- Toàn bộ 12 client kết nối, xác thực thành công và giữ kết nối trong 10 giây.
- **Quan sát màn hình Server:** Server sẽ in bảng **Active Users Table** hiển thị đầy đủ 12 client (Username, IP, Active Port) cùng online đồng thời.
- Sau 10 giây, toàn bộ client tự động gửi `BYE` ngắt kết nối an toàn và dọn dẹp state sạch sẽ.
