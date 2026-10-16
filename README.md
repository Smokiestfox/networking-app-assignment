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

## Cách chạy và Kiểm thử

### 1. Chạy Unit Tests

Chạy toàn bộ test suite bằng `pytest`:

```bash
python -m pytest tests/ -v
```

Hoặc chạy riêng từng bài test:

```bash
# Test tầng giao thức (frame packing/unpacking)
python -m pytest tests/test_protocol.py -v

# Test Multi-threading Server, Auth Lifecycle & Active Users
python -m pytest tests/test_server.py -v
```

### 2. Chạy Trực tiếp Client – Server

**Bước 1: Khởi chạy Server (Terminal 1)**

```bash
python server.py
```

Server sẽ lắng nghe tại `0.0.0.0:9000` (theo `config.py`) và đọc tài khoản từ `users.json`.

**Bước 2: Khởi chạy Client (Terminal 2)**

Chạy client mặc định (user `alice`, pass `123456`):
```bash
python client.py
```

Hoặc chỉ định tài khoản khác trong `users.json`:
```bash
python client.py bob 123456
python client.py charlie 123456
python client.py admin admin
```

**Luồng hoạt động:**
1. Client kết nối TCP tới server.
2. Gửi thông điệp `AUTH` kèm username & password.
3. Server xác thực qua `UserRegistry`, đăng ký vào bảng `Active Users` (atomic), trả về `AUTH_OK` kèm danh sách user online (hoặc `AUTH_FAIL` nếu sai thông tin / đã đăng nhập).
4. Client nhận `AUTH_OK`, gửi lệnh `USER_LIST` để xem danh sách user online cập nhật.
5. Client gửi `BYE` để đăng xuất an toàn (graceful disconnect). Server trả `ACK`, gỡ client khỏi `Active Users` và đóng socket.
