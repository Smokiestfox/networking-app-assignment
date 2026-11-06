# Đối chiếu yêu cầu Assignment 1 và streaprous

Ngày cập nhật: **06/11/2026**. Người tổng hợp: **Chính**.

Mốc source đối chiếu: commit `ca46569` trên nhánh `feat/week5-client-api`.
Tài liệu ghi nhận hiện trạng để nhóm làm việc cuối tuần 07–08/11 và chốt hướng sau phản hồi dự kiến vào Thứ Hai 09/11. Tại thời điểm tổng hợp, nhóm chưa cung cấp phản hồi của thầy cho email hỏi về đề mới. Các mục “chờ xác nhận” chưa phải quyết định của giảng viên.

## 1. Nguồn đối chiếu

Các file đề nằm trong thư mục `đề_update_md/` của workspace, ngoài Git repo hiện tại. Tên mục/trang dưới đây dùng để tra cứu bản tài liệu đã nhận.

| Mã | Nguồn | Phần sử dụng |
| --- | --- | --- |
| S1 | `Assignment1.md` | Mục 4–8: chức năng, transport, flow/congestion, security, môi trường; mục 9: rubric 10 bước; mục 10: nộp bài. |
| S2 | `Assignment1_Network_Application_Guide.md` | Slide 7: điểm chưa nhất quán về TCP/UDP; slide 16, 18: window/flow/congestion; slide 21–22: security và HTTP UI. |
| S3 | `giai_dap_thac_mac.md` | Bài giải đáp ghi ngày 12/10/2026, sáu câu trả lời về TCP/UDP, handshake, CUBIC, security, HTTP UI và discovery. |
| S4 | `CO3094_assignment_streaprous.md` | Đề ghi ngày 30/10/2026; mục 2.1–2.4: raw stream, HTTP authentication, hybrid P2P chat; mục 3.2–3.3: yêu cầu/cách chấm theo Assignment 1. |
| S5 | `project_full_CO3094_streaprous.md` | Bản export framework: README, `streaprous_raw_connection.py`, HTTP adapter và cấu hình chạy. Chỉ đọc source, chưa chạy framework trong lần đối chiếu này. |
| S6 | Email thầy “Re: CO3093-CO3094 Computer Network HTTP over streaming socket” | Nội dung do Chính cung cấp: framework có accept/send/receive; sinh viên triển khai reliable, flow control, congestion control. Chưa có chỉ dẫn rõ tầng triển khai trong email. |
| S7 | Email Chính đã gửi hỏi thầy | Năm nhóm câu hỏi về quan hệ hai đề, rubric, tầng transport, relay/P2P, môi trường demo; bổ sung câu hỏi hạn nộp. Chưa có câu trả lời được cung cấp. |

Không dùng source mẫu cũ để suy ra các yêu cầu mới. S3 có trước S4; những điểm S3 và S4 khác nhau cần được thầy xác nhận, không tự mặc định một tài liệu đã hủy tài liệu còn lại.

## 2. Bảng đối chiếu yêu cầu và hiện trạng

“Đã có” nghĩa là có triển khai trong source tại mốc trên, không đồng nghĩa đã được giảng viên nghiệm thu hoặc đạt điểm rubric.

| Nội dung | Đề cũ và giải đáp | Đề/framework/email mới | Source hiện tại và phần còn thiếu |
| --- | --- | --- | --- |
| **Rubric và phạm vi** | S1 có 10 milestone, mỗi milestone 1 điểm. | S4 mục 3.2–3.3 dẫn yêu cầu và grading về Assignment 1. | Có cơ sở tiếp tục đối chiếu rubric cũ, nhưng chưa rõ streaprous thay thế, bổ sung toàn bài hay thuộc M9; chưa rõ cách tính P2P và ba cơ chế lý thuyết. |
| **Thiết kế protocol — M1** | S1 yêu cầu packet structure, message types và state diagram trước khi code; kiến trúc tách network/data/UI. | S4 yêu cầu thiết kế giao thức và quy trình xử lý giữa các tiến trình/peer. | Có header UDP 37 B, TCP length-prefix và tài liệu protocol/architecture. Nếu chọn kiến trúc mới, phải bổ sung state/sequence và giao thức HTTP/P2P/raw stream theo flow thực tế, không dùng nguyên sơ đồ relay cũ để mô tả P2P. |
| **Socket và transport** | S1/S2 có chỗ diễn đạt TCP/UDP khác nhau; S3 hướng dẫn TCP cho handshake, authentication và chat; UDP cho file và heartbeat/discovery. | S4 mô tả raw TCP byte stream giữa daemon; S5 có `SOCK_RAW`/`IPPROTO_TCP`. | `ClientSession` dùng TCP `SOCK_STREAM` cho control và UDP `SOCK_DGRAM` cho chat/file. Chưa có lớp raw TCP của streaprous. Socket TCP do hệ điều hành quản lý không đồng nghĩa raw TCP trong framework mới. |
| **Handshake — M2** | Kết nối TCP và nhận ACK; S3 chấp nhận handshake tầng ứng dụng, không bắt buộc thêm key exchange. | S4 mục 2.1 mô tả handshake ba bước TCP ở raw connection. | Có `HELLO → ACK`, `AUTH → AUTH_OK/AUTH_FAIL`. Chưa tích hợp raw handshake. Cần xác nhận có phải triển khai cả hai lớp hay được giữ flow hiện tại. |
| **Authentication/state — M3** | Đăng nhập, theo dõi username/IP/port; S3 chấp nhận registry tập trung. | S4 mục 2.2 yêu cầu HTTP authentication, trình bày hướng headers hoặc cookies. | Có Python AUTH và registry; chưa có browser login/HTTP authentication. Đăng nhập bằng CLI rồi mở UI chưa thay thế yêu cầu HTTP authentication nếu S4 áp dụng đầy đủ. |
| **Chat và định tuyến — M5** | S1 M5 quy định client gửi server, server broadcast/forward đúng người; S3 hướng dẫn chat qua TCP. | S4 mục 2.3 yêu cầu lấy danh sách từ tracker, tạo kết nối trực tiếp; khi chat không đi qua server trung tâm. | Có chat chung/riêng **qua UDP relay server**, chưa có ACK giao nhận chat và chưa có kết nối chat P2P. Có hai điểm cần chốt riêng: TCP/UDP và relay/P2P. |
| **Discovery/tracker** | S3 cho phép client đăng ký, UDP heartbeat, server giữ `last_seen` và cập nhật danh sách; không bắt buộc broadcast presence trực tiếp trên LAN. | S4 yêu cầu đăng ký IP/port, tracker active peers, lấy danh sách để kết nối P2P. | Có registry, heartbeat và user list. Client khai báo UDP port trước AUTH. Chưa có peer listener/địa chỉ phục vụ kết nối chat P2P; chưa hoàn thành phần connection setup của S4. |
| **Reliable file — M6** | S1 M6 yêu cầu file qua UDP; S3 nêu sequence, ACK, retransmission, chunking, reassembly và sliding window tầng ứng dụng. | S6 yêu cầu tự làm reliable; chưa nói rõ áp dụng cho raw stream, UDP file hay cả hai. S4 không làm rõ đường truyền file. | Có chunk 700 B, ACK, gửi lại theo RTO, ghép theo index, SHA-256; file relay qua server. Chưa có reliability hoàn chỉnh trong raw TCP. Cần thầy xác nhận phần UDP được giữ/tính điểm thế nào. |
| **Flow control** | Đã có trong S1 mục 6 và S2 slide 16, 18; không phải yêu cầu chỉ mới xuất hiện trong email. | S6 nêu flow control riêng với reliable và congestion control. | Sender giới hạn inflight theo `cwnd`; receiver có giới hạn số phiên. Chưa có receiver-advertised window (`rwnd`) phản ánh bộ đệm còn trống, cập nhật cửa sổ và xử lý cửa sổ bằng 0. Đây là khoảng trống cần thiết kế; tầng triển khai chờ xác nhận. |
| **Congestion control** | S1 nêu BBR/CUBIC; S3 xác nhận CUBIC đủ nếu giải thích và demo được hành vi, không bắt buộc BBR. | S6 yêu cầu tự làm congestion control; chưa chỉ rõ thuật toán hoặc tầng triển khai. | Có CUBIC, slow start, giảm cwnd khi mất gói, RTT/RTO cho UDP FileSender. Chưa áp dụng vào raw stream; không tự kết luận bản UDP đã đáp ứng email mới. |
| **Concurrency/resilience — M4, M7** | Nhiều client đồng thời; không chặn luồng chính; xử lý timeout/disconnect. | S4 yêu cầu giao tiếp non-blocking giữa nhiều daemon, gợi ý callback/coroutine. | Có server đa luồng; client có TCP reader, UDP reader, heartbeat và file worker; API/callback, EOF/PING timeout. Đây là nền tảng hiện tại, chưa chứng minh giao tiếp non-blocking giữa peer daemon theo S4. |
| **Security — M8** | Mã hóa payload bằng thư viện chuẩn về cryptography; S3 cho phép `cryptography`, không bắt buộc `ssl` hoặc certificate. | S4 bổ sung HTTP authentication và yêu cầu backend dùng thư viện chuẩn Python; cần làm rõ phạm vi ngoại lệ cho cryptography nếu áp dụng đồng thời. | UDP payload có Fernet; TCP framing hiện vẫn gửi JSON plaintext; khóa còn hard-code. Server còn log nguyên request và cập nhật endpoint từ username trong HEARTBEAT/MSG. Chưa nghiệm thu security toàn hệ thống. |
| **HTTP UI — M9** | S3 xác nhận nên làm HTTP UI để đạt M9, cho phép `http.server`/`BaseHTTPRequestHandler` phục vụ UI; không cần tự viết HTTP bằng raw socket chỉ cho giao diện. | S4 mô tả HTTP server/webapp streaprous, backend Python, JavaScript chỉ phía client, không dùng web framework có sẵn. | Có ClientSession API, JSON snapshot, mock và hook `--web-port`; chưa có `web_gateway.py` hoặc UI trong checkout. Gateway tạm theo S3 chưa chứng minh đáp ứng transport của S4. |
| **Channel và thông báo** | S1 nêu global/private chat và hiển thị messages/users/file progress. | S4 có danh sách channel đã tham gia, cửa sổ cuộn, nhập/gửi tin, tin không sửa/xóa, thông báo tin mới; access control channel là tùy chọn. | Chưa có mô hình channel/UI tương ứng. Cần chốt phạm vi triển khai nếu đề mới áp dụng đầy đủ; không tự thêm quản trị channel/ACL vượt yêu cầu. |
| **Môi trường demo** | S1 cho chạy trên hệ điều hành thông dụng, localhost/local IP. | README S5 hướng dẫn Linux/sudo; source tạo raw TCP và có cấu hình chặn kernel RST bằng `iptables` trên nhánh Linux. | Bản hiện tại đã kiểm tra localhost Windows. Chưa chạy framework mới hoặc kiểm chứng Linux VM/browser Windows. OS máy chấm và việc chấp nhận VM chờ thầy xác nhận. |
| **Testing/report/nộp bài — M10** | S1 yêu cầu source, README chạy từng bước, report PDF, kiến trúc, protocol, sequence và challenges. | S4 yêu cầu coding style PEP 8/257 và tiếp tục dẫn về Assignment 1. | Có pytest, simulator và tài liệu Markdown. Cần ghép test/bằng chứng UI-security và cập nhật report theo kiến trúc được chốt; không dùng kết quả test cũ để suy ra đã đạt đề mới. |

## 3. Phần có thể giữ lại và giới hạn tái sử dụng

- **Client API/state/UI contract:** [client.py](../client.py), [client_api.md](client_api.md), [mock_state.json](../mock_state.json). Gateway/UI dùng API và schema đã bàn giao; nếu đổi transport thì sửa phần kết nối phía dưới. Chưa bảo đảm contract hiện tại đủ cho channel và HTTP login mới.
- **Reliable file và congestion:** [file_sender.py](../file_sender.py), [file_receiver.py](../file_receiver.py), [congestion.py](../congestion.py), [file_transfers.py](../file_transfers.py). Giữ làm bản hoạt động và tham khảo thuật toán. Packet UDP theo mảnh và raw TCP theo byte có semantics ACK/sequence khác nhau; không sao chép nguyên trạng rồi coi là raw transport hoàn chỉnh.
- **Registry/discovery, test và simulator:** giữ logic phù hợp cùng kịch bản loss/disconnect/SHA-256. Nếu chuyển P2P, phải thay cách tìm peer/kết nối và kiểm chứng lại đường truyền thực tế.
- **Security:** codec Fernet có thể tiếp tục phục vụ giao thức Python khi được chấp nhận. Không mã hóa trực tiếp HTTP browser bằng Fernet; HTTP authentication và bảo vệ nội dung truyền là các yêu cầu riêng.

Framework mới là nền tảng cần hoàn thiện, không phải bằng chứng nhóm đã có đủ transport: trong S5, `run_conn_send()` gửi segment rồi tăng sequence; đường nhận ACK/trả payload chưa thể hiện đầy đủ cơ chế ghép stream đúng thứ tự/loại trùng; TCP window còn cố định `5840`. Nhận xét này dựa trên source export, chưa phải kết quả thực nghiệm của framework.

## 4. Phần thiếu đã xác định và việc chờ quyết định

| Loại | Phần còn lại | Hướng xử lý trước khi thầy trả lời |
| --- | --- | --- |
| Có thể tiếp tục | Gateway/UI thật, mã hóa TCP framing Python, cấu hình khóa, che mật khẩu log, ràng buộc endpoint, kiểm thử và bằng chứng demo. | Làm theo kế hoạch tuần 5 v3; chỉ nghiệm thu phần thực sự chạy được trên bản hiện tại. |
| Có thể chuẩn bị | Thiết kế flow control: giới hạn bộ đệm, rwnd, đơn vị, cập nhật/mở lại cửa sổ, receiver chậm và cửa sổ bằng 0. | Hậu thiết kế ngắn, Chính review; chưa thay wire protocol khi tầng triển khai chưa chốt. |
| Cần kiểm chứng môi trường | Framework streaprous chạy đến đâu; TODO nào đang chặn; browser truy cập ra sao. | Bình thử tối đa nửa ngày trong môi trường/source riêng, bàn giao lệnh và ảnh/log, kể cả kết quả chưa chạy được. |
| Chờ chốt kiến trúc | Chat TCP/UDP, relay/P2P, reliable/flow/congestion trong raw connection hay UDP hoặc cả hai. | Ghi phương án và tác động; giữ bản hiện tại, chưa viết lại transport hoặc chuyển routing. |
| Chờ chốt phạm vi mới | HTTP authentication, channel, notification, proxy/backend và quy mô triển khai tối thiểu; phạm vi cho phép thư viện cryptography. | Ghi nhận là yêu cầu trong S4, chưa kết luận được miễn hoặc phải hoàn thành ngay cuối tuần. |

### Câu hỏi Chính đã gửi thầy

| Mã | Cần xác nhận | Căn cứ hiện có và quyết định còn thiếu |
| --- | --- | --- |
| Q1 | Streaprous thay thế, bổ sung hay chỉ liên quan M9 của Assignment 1? | S4 dẫn về Assignment 1 nhưng thêm raw transport/P2P/HTTP authentication. Chưa đủ để tự chọn một trong ba cách hiểu. |
| Q2 | Vẫn chấm rubric 10 bước? Reliable, flow, congestion và tracker/P2P tính ở bước nào? | S4 mục 3.3 nói chấm theo Assignment 1; chưa có mapping chi tiết. Không tự gán mỗi chức năng mới một điểm. |
| Q3 | Ba cơ chế phải nằm trong `streaprous_raw_connection` hay tầng ứng dụng UDP? UDP reliable file/CUBIC hiện tại có được giữ và tính điểm? | S3 cho phép reliability UDP và CUBIC; S6 yêu cầu ba cơ chế nhưng chưa chỉ tầng. Câu trả lời quyết định phần phải triển khai lại. |
| Q4 | Chat/file qua server hay trực tiếp P2P sẽ được chấm? | S1 M5 yêu cầu chat server relay; S4 yêu cầu chat trực tiếp. Đường truyền file cần xác nhận riêng, không suy từ yêu cầu P2P chat. |
| Q5 | OS máy chấm/demo; Linux VM VirtualBox Host-Only với browser trên Windows có được chấp nhận? | S5 hướng dẫn Linux/sudo và có cấu hình RST. Chưa có xác nhận phương án VM hoặc kết quả thử của nhóm. |
| Q6 | Hạn nộp chính thức là ngày nào? | S1 ghi “Suggested Deadline: Sunday, November 17, 2026, at 11:59 PM”; Chính báo LMS hiển thị 04/12/2026. Thông tin LMS là do Chính cung cấp, chưa kiểm tra trực tiếp; chờ thầy xác nhận ngày/giờ/múi giờ áp dụng. |

Sau Q1, nhóm cần chốt phạm vi HTTP authentication/channel và cách dùng thư viện cùng các câu trả lời Q2–Q5. Không coi việc chưa được trả lời là chấp thuận bỏ yêu cầu.

## 5. Bằng chứng hiện có và giới hạn kiểm chứng

Kiểm chứng trước khi push `ca46569`, ngày **06/11/2026**:

- Bộ test trong repo: `python -B -m pytest -q -p no:cacheprovider --tb=short` — **58 passed, 31.25 s**. Các file test không thay đổi trong commit Client API.
- Kịch bản kiểm tra riêng ngoài repo, đã chạy trong phiên làm việc: ba client TCP/UDP thật; file **1 MiB** đúng SHA-256; chat đúng người; tiến độ hai phía; năm `refresh_users()` đồng thời; snapshot tách khỏi state nội bộ; khai báo UDP port trong AUTH.
- Kịch bản riêng cũng kiểm tra chunk trùng không tăng tiến độ, checksum sai, file rỗng, đường dẫn thiếu, TCP EOF, server không trả PONG và CLI gửi đường dẫn có dấu cách.
- Gateway hook chỉ được kiểm tra bằng handle thay thế theo contract. **Chưa có kiểm chứng HTTP/UI thật, raw TCP streaprous, P2P chat hoặc flow control theo rwnd.**

Kịch bản riêng chưa được đưa vào bộ test versioned của repo; Quỳnh cần tiếp nhận kịch bản và lưu kết quả cần thiết để nhóm tái hiện. Tên test không thay thế bằng chứng về tham số: `test_one_mb_file_transfer_with_loss` hiện dùng **512 KiB và loss 8%**, không chứng minh riêng ca 1 MiB/loss 20%. Kiểm tra ba ClientSession không thay thế bài test tải 10–20 client của S1. Các kiểm tra localhost hiện có cũng chưa chứng minh hành vi trên LAN/VM hoặc toàn bộ M8/M9 mới.

## 6. Mốc xử lý theo kế hoạch tuần 5 v3

Giờ Việt Nam (UTC+7). Các mốc dưới đây là kế hoạch bàn giao, không phải trạng thái hoàn thành.

| Mốc | Người phụ trách | Đầu ra |
| --- | --- | --- |
| 07/11, 12:00 | Chính; Bình | Bàn giao API/mock/contract; báo cáo thử framework tối đa nửa ngày. Client API đã push ở `ca46569`, việc tích hợp thực tế vẫn còn. |
| 07/11, 18:00 | Hậu, Bình, Quỳnh; Chính review | Security/gateway/UI chạy độc lập; thiết kế flow control; bảng đối chiếu đề và danh sách quyết định chờ thầy. |
| 08/11, 12:00 | Quỳnh phối hợp Bình/Chính | UI ghép API thật. |
| 08/11, 16:00 | Chính và cả nhóm | Bản tích hợp, kết quả test/capture/demo, README/architecture theo hành vi đã kiểm chứng. |
| Sau phản hồi dự kiến 09/11 | Chính và cả nhóm | Cập nhật quyết định Q1–Q6, chọn kiến trúc và phân công phần còn lại. Nếu chưa có phản hồi, các câu hỏi vẫn mở. |

**Cách cập nhật sau khi thầy trả lời:** ghi ngày và nguồn phản hồi, trả lời từng Q1–Q6, sửa trạng thái trong bảng đối chiếu và kế hoạch tiếp theo. Giữ mốc source/kiểm chứng cũ để tránh nhầm phần đã có với phần mới đang triển khai.
