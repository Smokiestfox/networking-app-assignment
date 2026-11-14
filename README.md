# Networking App Assignment

Ứng dụng cộng tác client-server tự host cho mạng LAN/VPN: đăng nhập, quản lý phiên, xem danh sách user online, chat chung (broadcast) và chat riêng (unicast), truyền file tin cậy trên UDP kèm kiểm tra toàn vẹn SHA-256 và điều khiển tắc nghẽn CUBIC, có mã hóa payload. Toàn bộ phần mạng viết bằng raw socket tiêu chuẩn (không sử dụng framework cấp cao như Flask, Django hay WebSocket).

Môn học: Network Application Development (MMT-BTL).

---

## 1. Cấu trúc thư mục

```
.
├── client.py            # ClientSession API, state/callback và CLI tương thích
├── server.py            # Server quản lý kết nối TCP, định tuyến UDP, relay file và dọn dẹp phiên
├── web_gateway.py       # HTTP Gateway raw socket: API state, chat, auth, file transfer và SSE push
├── protocol.py          # Cấu trúc packet (37-byte header), framing TCP, mã hóa Fernet & helper đóng/mở gói
├── config.py            # Cấu hình IP, TCP/UDP port, timeout, chunk size, window size
├── registry.py          # Quản lý danh sách active users thread-safe
├── congestion.py        # Điều khiển tắc nghẽn CUBIC & bộ ước lượng RTT/RTO (RFC 6298)
├── file_sender.py       # Phía gửi file: chia chunk 700 B, sliding window CUBIC, selective retransmission
├── file_receiver.py     # Phía nhận file: ghép mảnh, phản hồi ACK, kiểm tra SHA-256 & lưu file
├── file_transfers.py    # Luồng worker nền xử lý file I/O không làm nghẽn chat/heartbeat
├── simulate_demo.py     # Script mô phỏng kiểm tra mạng tự động (hỗ trợ chế độ TCP, UDP, FILE)
├── mock_state.json      # Snapshot v1 mẫu để làm UI độc lập với gateway
├── users.json           # Dữ liệu tài khoản mẫu phục vụ kiểm thử
├── requirements.txt     # Danh sách thư viện phụ thuộc
├── ui/                  # Giao diện Web Dashboard (HTML5, Vanilla JS, CSS responsive)
│   ├── index.html       # Giao diện chính: Login Card, Chat Room, File Transfer & Status
│   ├── app.js           # Xử lý sự kiện UI, SSE Real-time, upload Base64 & download file
│   └── style.css        # Hệ thống styling hiện đại, responsive layout & animation
├── docs/
│   ├── architecture.md  # Tài liệu kiến trúc hệ thống 3 lớp & luồng đa luồng
│   ├── protocol.md      # Đặc tả giao thức chi tiết, sơ đồ trạng thái & sequence diagrams
│   ├── client_api.md    # API client và contract HTTP/JSON tuần 5
│   └── web_gateway.md   # Hướng dẫn chi tiết & bảo mật raw-socket HTTP Gateway
└── tests/               # Bộ kiểm thử tự động (pytest)
```

---

## 2. Hướng dẫn cài đặt môi trường (Python 3.11+)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

*(Trên macOS / Linux: dùng `source .venv/bin/activate`)*

---

## 3. Chạy Unit Tests & Integration Tests

Kiểm tra toàn bộ giao thức (protocol), xác thực, điều khiển tắc nghẽn CUBIC, và truyền file tin cậy:

```bash
python -m pytest tests -v
```

Hoặc chạy nhanh chế độ tóm tắt:

```bash
python -m pytest -q
```

---

## 4. Hướng dẫn chạy tương tác Client – Server

### Bước 1: Khởi chạy Server (Terminal 1)

```bash
python server.py
```

Server sẽ lắng nghe trên cổng `9000` (TCP control channel) và `9001` (UDP data channel), đồng thời ghi log bảng Active Users thời gian thực.

### Bước 2: Khởi chạy các Client (Terminal 2, 3)

```bash
# Đăng nhập với tài khoản mẫu (xem users.json):
python client.py bob pass123

# Mở thêm terminal khác cho Alice:
python client.py alice pass123
```

- **Lệnh tương tác trong Client CLI:**
  - `/list` : Xem danh sách user đang online kèm endpoint.
  - `/all <nội dung>` : Gửi tin nhắn broadcast đến tất cả mọi người.
  - `/to <username> <nội dung>` : Gửi tin nhắn unicast riêng cho một người dùng.
  - `/send <username> <đường dẫn file>` : Truyền file tin cậy ở chế độ nền (hỗ trợ đường dẫn có khoảng trắng được bọc trong dấu ngoặc kép).
  - `/quit` : Gửi `BYE` ngắt kết nối an toàn và đóng ứng dụng.

**Ví dụ truyền file:**

```text
alice> /send bob "C:\My Documents\report.pdf"
alice> /all Vẫn chat bình thường trong lúc file đang truyền!
```

- File nhận thành công sẽ được lưu tự động trong thư mục `downloads/<username>/`.
- Màn hình console in tiến độ dạng thanh `[#####.....] 52%`, log thông số `cwnd`/`RTO` mỗi giây, và xác nhận mã băm SHA-256 sau khi phía nhận đã kiểm tra toàn vẹn.

---

## 5. Chạy kịch bản mô phỏng kiểm thử tự động (`simulate_demo.py`)

Script hỗ trợ 3 chế độ kiểm thử mạng:

### a) Chế độ TCP – Concurrency Load Test (Rubric 3 & 4)

Khởi tạo đồng thời 12 client kết nối, xác thực và duy trì kết nối đồng thời:

```bash
python simulate_demo.py --mode tcp --count 12 --duration 10
```

### b) Chế độ UDP – Chat Messaging Test (Broadcast & Unicast)

Khởi tạo 5 client gửi nhận tin nhắn broadcast và unicast qua server:

```bash
python simulate_demo.py --mode udp --count 5 --duration 8
```

### c) Chế độ FILE – Reliable UDP File Transfer Demo

Khởi tạo 3 client thật (Alice gửi file cho Bob, Carol chat đồng thời liên tục):

```powershell
# 1. Truyền file sạch (không mất gói):
python simulate_demo.py --mode file --self-host --size-mb 5 --output downloads/demo-clean

# 2. Truyền file trong môi trường giả lập mất gói 15% (SIMULATE_LOSS=0.15):
python simulate_demo.py --mode file --self-host --size-mb 1 --loss 0.15 --timeout 240 --output downloads/demo-loss

# 3. Phía nhận (Bob) thoát đột ngột không gửi BYE (Alice tự động phát hiện và hủy phiên sạch sẽ):
python simulate_demo.py --mode file --self-host --size-mb 5 --abort-receiver --output downloads/demo-abort
```

Kết quả chi tiết được xuất ra file `<output>/result.json` kiểm tra đầy đủ tính toàn vẹn mã SHA-256, tính cách ly gói tin (Carol không nhận dữ liệu file gửi riêng cho Bob) và trạng thái các luồng dừng an toàn.

---

## 6. Tuần 5: Client API và điểm tích hợp Web UI

Phần client cung cấp `ClientSession` để gateway dùng chung một phiên TCP/UDP, cùng snapshot JSON cho UI. Xem [API và HTTP contract](docs/client_api.md); Quỳnh có thể phát triển giao diện bằng [mock_state.json](mock_state.json).

```python
from client import ClientSession

session = ClientSession("alice", "pass123")
try:
    if not session.start():
        raise RuntimeError(session.get_state()["status"]["detail"])
    session.send_chat("bob", "Chào Bob")
    print(session.get_state())
finally:
    session.close()
```

- `get_state()` trả bản sao JSON-safe gồm status, users, messages và transfers; không có password/key/socket.
- `send_file()` trả ID ngay và chạy I/O trong worker. Snapshot có tiến độ gửi/nhận thật, hoàn tất và lỗi; chunk trùng không làm tăng tiến độ nhận.
- TCP có một reader duy nhất cho USER_LIST/PONG/BYE. Client phát hiện TCP EOF/error hoặc mất phản hồi PING, chuyển status thành `error` và dừng worker.
- UDP được bind trước AUTH và `udp_port` được khai báo ngay khi đăng nhập, kể cả trong chế độ TCP simulator có dùng heartbeat.
- Chữ ký `run_client()` và các lệnh `/all`, `/to`, `/send`, `/list`, `/quit` được giữ tương thích.

Sau khi Bình hoàn tất `web_gateway.py` và Quỳnh hoàn tất giao diện `ui/`:

```powershell
# Chế độ 1: Đăng nhập từ CLI và kích hoạt Web Gateway trên cổng 8080
python client.py alice pass123 --web-port 8080
python client.py bob pass123 --web-port 8081

# Chế độ 2: Web-First Mode (Khởi chạy trực tiếp, đăng nhập/đăng xuất hoàn toàn trên trình duyệt)
python client.py --web-port 8080 --web-only
```

Mỗi tiến trình client có một gateway loopback và cổng riêng. Gateway được viết thuần 100% bằng **raw socket** (`socket` TCP + `threading`), hỗ trợ đầy đủ:
- `GET /api/state` & `POST /api/chat`: Xem trạng thái snapshot và gửi tin nhắn tức thì.
- `POST /api/login` & `POST /api/logout`: Quản lý phiên đăng nhập/đăng xuất động trực tiếp trên web UI.
- `POST /api/file/send` & `POST /api/file/upload`: Gửi tệp từ đường dẫn máy hoặc upload tệp Base64 từ trình duyệt (lên tới 50 MiB).
- `GET /api/file/download/<filename>`: Tải tệp tin nhận hoàn tất về máy tính.
- `GET /api/transfers`: Xem tiến độ truyền file và thông số kiểm soát nghẽn CUBIC (`cwnd`, `rto`, `progress`).
- `GET /api/events`: Luồng **Server-Sent Events (SSE)** đẩy cập nhật Real-time (0ms trễ) xuống giao diện Web.

Xem chi tiết kiến trúc, bảng mã lỗi và hướng dẫn tại [docs/web_gateway.md](docs/web_gateway.md).

Với server/cổng tùy chỉnh:

```powershell
python client.py alice pass123 127.0.0.1 9100 --udp-port 9101 --web-port 8080
```
