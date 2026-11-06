# Phase 1: Phiên bản Giao thức trên nền TCP

Thư mục này chứa toàn bộ mã nguồn của giai đoạn 1, nơi giao thức ứng dụng được xây dựng trên nền `socket.SOCK_STREAM` (TCP).

## Danh sách File
- `protocol.py`: Định nghĩa `Header` 10 bytes (`!IBIB`: `length`, `type`, `req_id`, `flags`) và hàm đọc/ghi frame theo luồng byte (`send_frame`, `recv_exact`, `recv_frame`).
- `client_connection.py`: Lớp `ClientConnection` quản lý trạng thái kết nối và khóa luồng gửi (`send_lock`) cho từng Client trên TCP Server.
- `tcp_server.py` & `tcp_client.py`: Máy chủ đa luồng (mỗi Client 1 thread) và máy khách hỗ trợ bắt tay `HELLO`/`WELCOME`, xác thực `AUTH`, `PING`/`PONG`, `MSG_BROADCAST` và `MSG_DIRECT`.
- `test_messaging.py`: Bộ kiểm thử tự động kiểm tra 3 kịch bản: truyền đồng thời tải lớn (`test_concurrent_broadcasts` - 20 tin nhắn 16KB mỗi luồng), xử lý người nhận ngắt kết nối (`test_disconnected_recipient`), và điều kiện tranh chấp khi ngắt kết nối (`test_disconnect_race`).

## Cách chạy độc lập
```bash
# Chạy TCP Server
python phase1_tcp/tcp_server.py

# Chạy TCP Client
python phase1_tcp/tcp_client.py

# Chạy Automated Stress Test (khi tcp_server.py đang bật)
python phase1_tcp/test_messaging.py
```