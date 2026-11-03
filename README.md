# Networking App Assignment

Ứng dụng cộng tác client-server tự host cho mạng LAN/VPN: đăng nhập, quản lý phiên, xem danh sách user online, chat chung (broadcast) và chat riêng (unicast), truyền file tin cậy trên UDP kèm kiểm tra toàn vẹn SHA-256 và điều khiển tắc nghẽn CUBIC, có mã hóa payload. Toàn bộ phần mạng viết bằng raw socket tiêu chuẩn (không sử dụng framework cấp cao như Flask, Django hay WebSocket).

Môn học: Network Application Development (MMT-BTL).

---

## 1. Cấu trúc thư mục

```
.
├── client.py            # Client TCP/UDP & giao diện dòng lệnh (CLI)
├── server.py            # Server quản lý kết nối TCP, định tuyến UDP, relay file và dọn dẹp phiên
├── protocol.py          # Cấu trúc packet (37-byte header), framing TCP, mã hóa Fernet & helper đóng/mở gói
├── config.py            # Cấu hình IP, TCP/UDP port, timeout, chunk size, window size
├── registry.py          # Quản lý danh sách active users thread-safe
├── congestion.py        # Điều khiển tắc nghẽn CUBIC & bộ ước lượng RTT/RTO (RFC 6298)
├── file_sender.py       # Phía gửi file: chia chunk 700 B, sliding window CUBIC, selective retransmission
├── file_receiver.py     # Phía nhận file: ghép mảnh, phản hồi ACK, kiểm tra SHA-256 & lưu file
├── file_transfers.py    # Luồng worker nền xử lý file I/O không làm nghẽn chat/heartbeat
├── simulate_demo.py     # Script mô phỏng kiểm tra mạng tự động (hỗ trợ chế độ TCP, UDP, FILE)
├── users.json           # Dữ liệu tài khoản mẫu phục vụ kiểm thử
├── requirements.txt     # Danh sách thư viện phụ thuộc
├── docs/
│   ├── architecture.md  # Tài liệu kiến trúc hệ thống 3 lớp & luồng đa luồng
│   └── protocol.md      # Đặc tả giao thức chi tiết, sơ đồ trạng thái & sequence diagrams
├── tests/               # Bộ kiểm thử tự động (pytest)
└── tools/
    └── export_project.py # Công cụ đóng gói mã nguồn phục vụ nộp bài
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

Kết quả chi tiết được xuất ra file `<output>/result.json` kiểm tra đầy đủ tính toàn vẹn mã SHA-256, tính cách ly gói tin (Alice không nhận gói file của Bob) và trạng thái các luồng dừng an toàn.
