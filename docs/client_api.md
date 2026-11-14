# Client API và HTTP contract tuần 5

Schema v1 là giao diện dùng chung giữa phần client của Chính, gateway của Bình và UI của Quỳnh. `mock_state.json` ở gốc repo là dữ liệu minh họa cùng schema; các file/checksum trong mock không phải kết quả truyền file thật.

## Phần đã triển khai và điểm tích hợp

- `client.ClientSession`: chạy TCP/UDP, quản lý trạng thái, chat, file và callback.
- `file_transfers.FileTransfers`: gom sự kiện file; dùng FileSender/FileReceiver hiện có.
- `run_client()` giữ nguyên danh sách tham số tuần 4. Các chế độ simulator cũ tiếp tục gọi được.
- `--web-port` là điểm gọi gateway. Bình cần cung cấp `web_gateway.start_gateway()` theo contract bên dưới. Checkout chưa có module đó thì CLI báo rõ và có thể chạy tiếp bằng cách bỏ cờ.
- HTTP server/parser, static UI và các thay đổi mã hóa/server hardening thuộc phần các thành viên tương ứng. Client gọi helper framing hiện có; không tự thay cách mã hóa của protocol.

## API Python cho gateway

```python
from client import ClientSession

session = ClientSession("alice", "pass123", host="127.0.0.1",
                        port=9000, server_udp_port=9001)
if not session.start():
    raise RuntimeError(session.get_state()["status"]["detail"])

state = session.get_state()
message_id = session.send_chat("bob", "Xin chào")
transfer_id = session.send_file("bob", "report.pdf")  # hiện dùng từ CLI/Python
session.close()
```

| API | Hành vi |
| --- | --- |
| `start() -> bool` | Bind UDP trước AUTH; gửi `udp_port`, handshake/auth TCP, khởi chạy các worker. Trả False và lưu trạng thái `error` nếu không kết nối/đăng nhập được. Gọi khi đã kết nối trả True. |
| `get_state() -> dict` | Snapshot JSON-safe, tách khỏi state nội bộ; đọc không tạo request mạng, không để lộ password, key hoặc socket. Gateway/UI không sửa trực tiếp nội bộ session. |
| `send_chat(to, text) -> str` | `to="*"` để chat chung, hoặc username khác. Trả UUID của tin sau khi gửi UDP. Ném ValueError nếu dữ liệu sai, ConnectionError nếu chưa kết nối. Tin outgoing là bản hiển thị cục bộ; chat hiện chưa có ACK giao nhận. |
| `send_file(to, path) -> str` | Xếp việc đọc/hash/gửi vào worker, trả UUID ngay; lỗi đường dẫn và kết quả truyền xuất hiện trong `transfers`/callback sau đó. File unicast, tối đa 8 send đang chờ/chạy. |
| `refresh_users(timeout=5) -> list` | Dành cho `/list`: chờ reply qua luồng TCP reader duy nhất. Gateway polling dùng `get_state()` để tránh tạo TCP request liên tục. |
| `close(graceful=True)` | Dừng gateway bên gọi trước, sau đó đóng session: BYE khi phù hợp, dừng file/heartbeat/reader và đóng socket. Gọi lặp lại an toàn. |
| `is_connected` | Thuộc tính bool của phiên client-server. |
| `wait(timeout=None) -> bool` | Đợi session ngừng; hữu ích cho script/headless. |

Callback tùy chọn: `ClientSession(..., on_event=handle_event)`. Callback nhận `{"type": "status|users|message|file", "data": ...}`; `data` cùng hình dạng trường/phần tử trong snapshot. Callback chạy trên thread phát sự kiện, nên chỉ copy/enqueue dữ liệu và trả về nhanh. Không thực hiện HTTP I/O, gọi `start/close` hoặc chờ `refresh_users` bên trong callback. Snapshot là nguồn đọc chính của gateway; callback không có replay.

## Snapshot v1

| Trường | Kiểu / quy ước |
| --- | --- |
| `schema_version` | Số nguyên, hiện là 1. |
| `revision` | Số nguyên tăng khi state thay đổi. Tuần 5 trả nguyên snapshot; chưa có cursor/history API. |
| `username` | Username của phiên này. |
| `status` | `{"state": ..., "detail": string|null}`. State: `disconnected`, `connecting`, `connected`, `closing`, `error`. |
| `users` | Mảng username đã sắp xếp, bao gồm bản thân khi đang online. Không bắt buộc UI hiển thị IP/port. |
| `messages` | Tối đa 100 tin gần đây, thứ tự cũ đến mới; mỗi tin có `id`, `from`, `to`, `text`, `time` (Unix seconds), `direction` (`incoming`/`outgoing`). |
| `transfers` | Mỗi phiên có schema bên dưới; giữ tối đa 50 bản ghi, ưu tiên giữ phiên đang hoạt động. |

UI có thể render lại snapshot hoặc upsert theo ID; không nối toàn bộ mảng `messages` sau mỗi lần poll vì sẽ lặp tin. Hiển thị nội dung chat/filename/error bằng text, không chèn thành HTML.

### Phiên file

| Trường | Quy ước |
| --- | --- |
| `id`, `direction`, `peer`, `filename` | UUID, `send`/`receive`, username đối tác, tên file. |
| `size` | Tổng byte; có thể null khi sender vừa được queue, chưa đọc metadata. |
| `bytes_transferred` | Sender: byte đã ACK. Receiver: byte từ các chunk mới mà receiver chấp nhận. Không tính lại chunk trùng. |
| `progress` | Số thực 0..1, không phải 0..100. Chỉ đạt 1.0 ở trạng thái `completed`. File rỗng vẫn hoàn tất 1.0 sau kiểm chứng/lưu thành công. |
| `status` | `queued`, `transferring`, `completed`, `failed`. Dùng field này để vẽ nhãn, không suy luận từ phần trăm đã làm tròn. |
| `error`, `path`, `checksum` | Chuỗi hoặc null. `path` chỉ có ở receiver khi lưu thành công. |
| `cwnd`, `rto` | Thông số sender, có thể null ở queued/receive. RTO tính bằng giây. |

Progress được phát tối đa khoảng 5 lần/giây/phiên và khi hoàn tất/lỗi. Console vẫn log cwnd/RTO khoảng mỗi giây. FileTransfers suy ra tiến độ nhận qua `on_packet()` trả True và callback `on_result()` hiện có, không đọc buffer/session nội bộ của receiver; vì vậy code của Hậu hiện tại vẫn dùng được. Callback `on_progress` mới của Hậu có thể được nối về cùng schema sau này mà không đổi API UI.

Khi peer rời hoặc client đóng, các phiên đang hoạt động phát lỗi. File chỉ hoàn tất khi cơ chế ACK/SHA-256 của FileSender/FileReceiver hiện có xác nhận thành công. Không có packet mã lỗi checksum trả ngược trong giao thức hiện tại: receiver có lỗi cụ thể, sender có thể chỉ báo timeout.

## Contract gateway cho Bình

```python
# web_gateway.py (Bình phụ trách)
def start_gateway(session=None, *, host="127.0.0.1", port=8080, ...):
    # Khởi chạy HTTP server trên background thread; trả về ngay.
    # Hỗ trợ cả chế độ dùng session truyền vào lẫn chế độ quản lý session động (Login/Logout).
    # Trả handle có phương thức close() để dừng server và nhả cổng.
    ...
```

CLI gọi factory sau khi client đã AUTH thành công (hoặc gọi ở chế độ `--web-only` với `session=None`) và gọi `handle.close()` khi thoát. Nếu bind HTTP thất bại, factory tự dọn dẹp phần khởi tạo dở rồi ném OSError/RuntimeError.

| Request | Response / ánh xạ |
| --- | --- |
| `GET /api/state` | 200 + nguyên snapshot `session.get_state()` JSON UTF-8, `Cache-Control: no-store`. Nếu chưa login, trả về state `disconnected`. |
| `POST /api/chat` | JSON `{"to":"bob","text":"Xin chào"}`; gọi `session.send_chat`. 202 + `{"message_id":"UUID"}` khi đã phát đi. 401 nếu chưa login. |
| `POST /api/login` | JSON `{"username":"alice","password":"123"}`; tạo phiên `ClientSession` và kết nối server. 200 khi thành công, 401 khi xác thực thất bại. |
| `POST /api/logout` | Đóng phiên làm việc hiện tại và đặt lại trạng thái chờ đăng nhập. 200 + `{"status":"ok"}`. |
| `POST /api/file/send` | JSON `{"to":"bob","path":"..."}`; gửi tệp từ đường dẫn máy. 202 + `{"transfer_id":"UUID"}`. |
| `POST /api/file/upload` | JSON `{"to":"bob","filename":"...","content":"<b64>"}`; upload tệp từ browser (hỗ trợ đến 50 MiB) và gửi P2P. |
| `GET /api/file/download/<file>` | Tải tệp nhận hoàn tất từ thư mục `downloads/` về máy. Chặn path traversal. |
| `GET /api/transfers` | Danh sách tiến độ truyền file và thông số kiểm soát nghẽn CUBIC (`progress`, `cwnd`, `rto`). |
| `GET /api/events` | Luồng **Server-Sent Events (SSE)** đẩy cập nhật Real-time (0ms trễ) mỗi khi có tin nhắn, user hay tiến độ file thay đổi. |
| Dữ liệu payload sai | 400 + `{"error":"Mô tả"}`. |
| Client chưa kết nối / socket lỗi | 503 + `{"error":"Mô tả"}`. |
| Sai method/route | 405/404 tương ứng. |

Xem tài liệu chi tiết tại [docs/web_gateway.md](web_gateway.md).

## Chạy và kiểm chứng

```powershell
# 1. Chạy CLI thuần túy (không web)
python client.py alice pass123

# 2. Chạy CLI kèm Web Gateway (dashboard trên cổng 8080)
python client.py alice pass123 --web-port 8080
python client.py bob pass123 --web-port 8081

# 3. Chạy Web-First Mode (đăng nhập trực tiếp trên trình duyệt)
python client.py --web-port 8080 --web-only

# 4. Server/custom ports
python client.py alice pass123 127.0.0.1 9100 --udp-port 9101 --web-port 8080
```

TCP reader duy nhất nhận USER_LIST/PONG/BYE; heartbeat gửi PING qua helper protocol hiện có. TCP EOF/error hoặc không có control response trong CLIENT_TIMEOUT khiến state chuyển `error` và worker dừng. Nếu sửa framing ở `protocol.py`, Hậu cần giữ `send_json_frame`/`recv_json_frame` tương thích và server hỗ trợ PING/PONG hiện có.

Các thay đổi mã hóa TCP, log và xác thực endpoint ở server chưa được thay thế bởi ClientSession. Chúng cần được tích hợp từ phần Hậu/Bình để nghiệm thu toàn bộ Security + UI.

## Kiểm chứng phần client (05/11/2026)

- Bộ test có sẵn: 58 test qua trước và sau thay đổi; không sửa các file test của nhóm.
- Ba ClientSession trên TCP/UDP thật: chat riêng đúng người, snapshot là bản sao, AUTH khai báo đúng UDP port; năm yêu cầu refresh_users đồng thời đều nhận kết quả qua một TCP reader.
- File 1 MiB có sự kiện tiến độ hai phía; file 5 MiB qua simulator đúng SHA-256, Bob nhận chat trong lúc truyền và Carol không nhận dữ liệu file. File 64 KiB mất 15% datagram đầu ra ở các client vẫn hoàn tất sau gửi lại.
- Chunk trùng không tăng tiến độ; file rỗng hoàn tất; checksum sai/đường dẫn thiếu phát lỗi; TCP EOF và server giữ socket mở nhưng không trả PONG đều chuyển session sang error. CLI gửi đường dẫn có dấu cách, /list và chat tiếp tục hoạt động. Các thread client dừng sau close.
- Điểm gọi start_gateway/close được kiểm tra bằng handle thay thế theo contract. Chưa xác minh HTTP/UI đầu-cuối vì gateway và UI chưa có trong checkout này.

Các lần chạy trên là kiểm tra localhost Windows, không phải benchmark LAN/WAN hoặc chứng nhận hoàn tất phần Security của cả nhóm.
