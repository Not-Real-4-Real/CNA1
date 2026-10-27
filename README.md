# Custom Reliable UDP Protocol (CNA1)

Hệ thống giao thức truyền tải tin cậy hoạt động ở tầng ứng dụng (User-space Transport Protocol) chạy trên nền UDP (`SOCK_DGRAM`), lấy cảm hứng từ kiến trúc của QUIC.

## 1. Kiến trúc 4 Tầng (4-Layer Architecture)
Mã nguồn trong `server.py`, `client.py` và `udp_protocol.py` được thiết kế tách biệt thành 4 tầng:
- **Layer 1 - Transport (`transport_send`, `transport_receive`):** Giao tiếp UDP socket thô với kích thước gói tối đa `MAX_DATAGRAM_SIZE = 1200` bytes (36 bytes cố định cho RDT + App Header, tối đa 1164 bytes cho JSON Payload).
- **Layer 2 - Reliable Delivery (`RDTHeader`, `SenderState`, `ReceiverState`):** Đảm bảo truyền dữ liệu tin cậy với Sequence Number, Packet-level ACK, Retransmission Timeout, Selective Repeat Sliding Window, Flow Control và Congestion Control.
- **Layer 3 - Session & Liveness (`Session`):** Quản lý phiên kết nối bằng `session_id` (16 bytes), bắt tay 2 bước (`OPEN` / `OPEN_ACK`), đóng kết nối (`CLOSE` / `CLOSE_ACK`) và cơ chế giữ sống kết nối một chiều (`HEARTBEAT`).
- **Layer 4 - Application (`AppHeader`):** Xử lý bản tin nghiệp vụ định dạng JSON UTF-8, phân biệt phản hồi đồng bộ qua `req_id` và sự kiện bất đồng bộ từ Server qua `EVENT_REQ_ID = 0`.

## 2. Cấu trúc Thư mục
```text
CNA1/
├── server.py               # Máy chủ UDP chính (Layer 1 -> Layer 4)
├── client.py               # Máy khách UDP chính (CLI + Daemon Receiver & Heartbeat)
├── udp_protocol.py         # Định nghĩa RDTHeader (26B), AppHeader (10B) và encode/decode
├── tools/
│   └── udp_loss_proxy.py   # Proxy trung gian giả lập đánh rớt gói tin UDP
├── test_assets/            # Các file mẫu (.txt, .png, .jpg, .docx) dùng để kiểm thử truyền file
├── phase1_tcp/             # Mã nguồn Phase 1 chạy trên nền TCP chuẩn (Baseline)
└── archived_udp/           # Các bản nháp UDP giai đoạn đầu và tài liệu phân tích lỗi
```

## 3. Hướng dẫn Chạy & Kiểm thử Nhanh

### Bước 1: Khởi động Server
```bash
python server.py
```
*(Server mặc định lắng nghe tại `127.0.0.1:5000`).*

### Bước 2: Khởi động Client và Đăng nhập
Mở một hoặc nhiều terminal khác và chạy:
```bash
python client.py
```
Sử dụng một trong 4 tài khoản được cấu hình sẵn trên Server để đăng nhập:

| Username | Password |
| :--- | :--- |
| `alice` | `alice123` |
| `bob` | `bob123` |
| `charlie` | `charlie123` |
| `doug` | `doug123` |

### Bước 3: Các lệnh hỗ trợ trên Client CLI
Sau khi xác thực thành công (`AUTH_OK`), bạn có thể sử dụng các lệnh sau:
- `peers`: Xem danh sách các người dùng đang trực tuyến.
- `ping`: Gửi gói tin `PING` kiểm tra độ trễ và nhận phản hồi `PONG`.
- `broadcast`: Gửi tin nhắn tới toàn bộ người dùng đang online.
- `direct`: Gửi tin nhắn riêng cho một username cụ thể.
- `upload`: Tải một file từ máy Client lên Server (file được cắt thành các chunk 600 bytes mã hóa Base64 và lưu vào thư mục `received_files/` trên Server).
  - *Ví dụ:* Nhập `test_assets/filebienso.png` (hoặc `filebienso.png` nếu để ở thư mục gốc).
- `sendfile`: Yêu cầu Server chuyển tiếp một file mà bạn đã `upload` cho một người dùng khác đang online (lưu vào `client_received_files/` của người nhận).
  - *Lưu ý quan trọng:* Ở mục `File path:` của lệnh `sendfile`, chỉ nhập **tên file gốc** đã lưu trên Server (ví dụ: `filebienso.png`), **không** nhập kèm đường dẫn thư mục `test_assets/`.
- `quit`: Đóng phiên làm việc (`CLOSE` / `CLOSE_ACK`) và thoát.