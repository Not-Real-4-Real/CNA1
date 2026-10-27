# Lưu trữ Phiên bản UDP Thử nghiệm Ban đầu

Thư mục này lưu giữ các bản dựng đầu tiên của giao thức UDP (`udp_server_old.py`, `udp_client_old.py`) cùng biên bản phân tích lỗi kiến trúc (`udp_problem.md`).

## Nguyên nhân Gốc rễ & Lý do Tái cấu trúc
1. **Lỗi thiếu luồng nhận nền liên tục (No Persistent Receiver Loop):**
   - Trong `udp_client_old.py`, việc đọc gói tin UDP (`receive_datagram`) bị nhốt bên trong vòng lặp của hàm `ping()`.
   - Khi Client nhận được `ACK` cho gói `PING` và nhận được gói `PONG` từ Server, nó gửi `ACK` cho `PONG` rồi kết thúc hàm `ping()` ngay lập tức để quay về chờ lệnh `input()` từ bàn phím.
   - **Kịch bản sập phiên:** Nếu gói `ACK` (cho `PONG`) từ Client gửi lên bị mất trên đường truyền, Server sẽ timeout và truyền lại gói `PONG` 5 lần (`MAX_RETRANSMISSIONS = 5`). Tuy nhiên, do Client đang bị chặn ở hàm `input()`, không có luồng nào đọc socket để gửi lại `ACK`, khiến Server đánh giá kết nối hỏng và xóa phiên (`invalidating session`).
2. **Thiếu hàng đợi `outbox` ở phía Server:**
   - Trong `udp_server_old.py`, nếu đang có 1 gói chờ `ACK` (`session.pending_data is not None`), mọi lệnh gửi tiếp theo bị từ chối thẳng (`[SEND BLOCKED]`) thay vì được xếp vào hàng đợi.

## Giải pháp đã áp dụng ở Phiên bản Chính (`server.py` & `client.py`)
- **Trên `client.py`:** Tách riêng luồng `receiver_loop` chạy nền liên tục (`daemon=True`) để luôn bắt và trả lời `ACK` cho mọi gói `DATA` (kể cả gói trùng lặp), đồng thời phân phối bản tin phản hồi về hàm gọi thông qua bảng băm `pending_requests` (`req_id -> queue.Queue`).
- **Trên `server.py`:** Bổ sung cấu trúc `SenderState.outbox` (`collections.deque`) và hàm `pump_outbox()` để không bao giờ làm rớt bản tin của tầng ứng dụng khi đường truyền đang bận.