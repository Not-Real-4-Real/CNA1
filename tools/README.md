# Công cụ Giả lập Mất gói tin (`udp_loss_proxy.py`)

Trên môi trường loopback (`127.0.0.1`), hệ điều hành hầu như không bao giờ làm rớt gói UDP. `udp_loss_proxy.py` đóng vai trò là một nút mạng trung gian đứng giữa Client (`127.0.0.1:5001`) và Server (`127.0.0.1:5000`) để chủ động đánh rớt gói tin theo kịch bản nhằm kiểm thử cơ chế truyền lại (Retransmission) và cửa sổ trượt.

## 1. Các chế độ đánh rớt gói (`DROP_MODE`)
Mở file `udp_loss_proxy.py` và chỉnh biến `DROP_MODE` theo kịch bản muốn kiểm thử (mặc định proxy sẽ đánh rớt **gói tin đầu tiên** khớp điều kiện):
- `"none"`: Chuyển tiếp toàn bộ gói tin, không đánh rớt.
- `"client_file_data"`: Đánh rớt gói `DATA` mang bản tin `FILE_DATA` từ Client gửi lên Server (kiểm thử Client retransmit dữ liệu).
- `"server_ack_after_file_data"`: Đánh rớt gói `ACK` từ Server trả về sau khi nhận `FILE_DATA` (kiểm thử cách Server xử lý gói trùng lặp khi Client gửi lại).
- `"client_ack_after_server_data"`: Đánh rớt gói `ACK` từ Client trả về sau khi nhận dữ liệu từ Server (kiểm thử Server retransmit).

## 2. Cách chạy Proxy (Tránh lỗi Import)
Để Python nhận diện đúng module `udp_protocol.py` ở thư mục gốc, hãy đứng tại thư mục gốc `CNA1/` và chạy bằng cờ `-m`:

1. **Terminal 1 (Server - Port 5000):**
   ```bash
   python server.py
   ```
2. **Terminal 2 (Proxy - Port 5001 -> 5000):**
   ```bash
   python -m tools.udp_loss_proxy
   ```
3. **Terminal 3 (Client):**
   Đổi hằng số `SERVER_PORT = 5001` trong `client.py`, sau đó chạy:
   ```bash
   python client.py
   ```