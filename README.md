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
