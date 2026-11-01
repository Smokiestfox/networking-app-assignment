# Networking App Assignment

Ứng dụng cộng tác client-server tự host cho mạng LAN/VPN: đăng nhập, xem danh sách user online, chat chung và chat riêng, truyền file tin cậy, có mã hóa payload. Toàn bộ phần mạng viết bằng raw socket (không dùng Flask, Django hay WebSocket).

Môn: Network Application Development, Assignment 1.

## Tính năng dự kiến

- Đăng nhập, server theo dõi user (tên, IP, port) và phát danh sách user online
- Chat chung (broadcast) và chat riêng (unicast) qua server
- Truyền file tin cậy trên UDP: chia chunk, sliding window, chỉ gửi lại gói thiếu, kiểm tra SHA-256
- Heartbeat, timeout, xử lý client thoát đột ngột
- Mã hóa payload hiện tại bằng Fernet (shared key cho bài thực hành)
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

### Cài môi trường (Python 3.11+)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\Activate.ps1
```

Trên macOS/Linux dùng `source .venv/bin/activate`. Các lệnh `python` bên dưới chạy trong môi trường đã kích hoạt.

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
  - `/list` để làm mới danh sách user online.
  - `/all <text>` chat chung, `/to <user> <text>` chat riêng.
  - `/send <user> <file>` truyền file ở nền, ví dụ `/send bob "C:\My Files\report.pdf"`.
  - `/quit` hoặc nhấn `Ctrl+C` để gửi `BYE` và kết thúc phiên an toàn.

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

## Tuần 4: reliable UDP file transfer

Hiện có `file_receiver_demo.py` để Chính phát triển/test FileSender và để Hậu tham khảo đồng bộ. Receiver chính thức `file_receiver.py` thuộc phần Hậu, chưa có trong nhánh này. Mặc định client vẫn chat/gửi được, nhận file chỉ bật khi có module chính thức hoặc chủ động thêm `--demo-receiver`. Xem [giao diện bàn giao cho Hậu](docs/file-receiver-handoff.md).

Chạy `python server.py`, sau đó mở `python client.py bob pass123 --demo-receiver` và `python client.py alice pass123 --demo-receiver` ở hai terminal. Đợi Bob đăng nhập, rồi tại Alice nhập:

```text
/send bob "C:\My Files\report.pdf"
/all Vẫn chat được trong lúc gửi file
```

File nhận nằm trong `downloads/bob/`, có UUID trước tên gốc. Console hiện tiến độ, cwnd/RTO mỗi giây, các đợt giảm cửa sổ khi timeout và kết quả SHA-256. Client chỉ báo `[FILE SENT]` sau khi receiver xác minh và lưu file. Gửi unicast, chunk 700 B, file tối đa 45,874,500 B; đường dẫn có dấu cách được hỗ trợ. Nếu Bob chưa có endpoint UDP, sender thử lại metadata đến khi có ACK hoặc hết hạn.

Demo tự động dùng server thật và 3 client Alice/Bob/Carol, tạo file nhị phân, kiểm tra SHA-256 và chat đồng thời:

```powershell
python simulate_demo.py --file-demo --demo-receiver --self-host --size-mb 5 --output downloads/demo-clean
python simulate_demo.py --file-demo --demo-receiver --self-host --size-mb 1 --loss 0.15 --timeout 240 --output downloads/demo-loss
python simulate_demo.py --file-demo --demo-receiver --self-host --size-mb 5 --abort-receiver --output downloads/demo-abort
```

Nếu server đã chạy, bỏ `--self-host`. Dùng `--host`, `--port`, `--udp-port` để đổi đích. Với self-host, có thể dùng `--port 0 --udp-port 0` để hệ điều hành chọn cổng trống. Demo ghi kết quả vào `<output>/result.json`, gồm receiver đang dùng; exit code 0 khi các điều kiện demo đạt, 1 khi thất bại. `--abort-receiver` đóng Bob không gửi BYE giữa chừng và kiểm tra Alice báo hủy. Sau khi Hậu bàn giao `file_receiver.py`, bỏ `--demo-receiver` để kiểm chứng module chính thức.

Giả lập mất gói bằng biến môi trường (đặt ở cả terminal Alice và Bob để mất cả dữ liệu lẫn ACK):

```powershell
$env:SIMULATE_LOSS = "0.15"
python client.py alice pass123 --demo-receiver
```

Trên macOS/Linux: `SIMULATE_LOSS=0.15 python client.py alice pass123 --demo-receiver`. Trong demo, `--loss` ghi đè biến môi trường. Mất gói chỉ áp dụng cho file; CUBIC/RTO có thể khiến demo mất gói chạy chậm, vì vậy cho phép timeout dài hơn. Trạng thái không có ACK mới vẫn bị hủy sau 30 s.

```powershell
python -m pytest -q
```

Test mặc định dùng receiver demo (`--file-receiver=demo`). Test mới gồm file rỗng, ~1 MB, mất 20%/trùng/đảo thứ tự bằng virtual clock, mất ACK metadata/cuối, checksum lỗi, receiver im lặng, và tích hợp TCP/UDP thật. Sau khi có code của Hậu, dùng `--file-receiver=production` để kiểm tra cùng contract. Xem đánh giá commit gốc và phạm vi kiểm chứng tại [review tuần 4](docs/reviews/week4-review.md).
