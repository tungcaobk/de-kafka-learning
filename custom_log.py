import logging
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger(module, file_name, log_dir='logs'):
    """
    Hàm để cấu hình logger, cắt file log theo ngày và lưu vào thư mục chỉ định.
    """
    # --- 1. Tạo thư mục log nếu nó chưa tồn tại ---
    if not os.path.exists(log_dir):
        # logging.info(f"Tạo thư mục log tại: {log_dir}")
        os.makedirs(log_dir)

    # --- 2. Định dạng cho tin nhắn log ---
    # Sử dụng format giống như bạn đã cung cấp
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # --- 3. Tạo logger chính ---
    # Sử dụng __name__ là một thói quen tốt để biết log đến từ module nào
    logger = logging.getLogger(module)
    logger.setLevel(logging.DEBUG) # Đặt mức log thấp nhất cho logger

    # --- 4. Cấu hình TimedRotatingFileHandler ---
    # Đây là phần quan trọng nhất để xoay vòng log file
    log_file_path = os.path.join(log_dir, file_name)
    
    # when='D': Xoay vòng theo ngày (Day). Các tùy chọn khác: 'S', 'M', 'H', 'midnight'
    # interval=1: Mỗi 1 ngày sẽ tạo một file mới.
    # backupCount=30: Giữ lại 30 file log cũ nhất. Các file cũ hơn sẽ bị xóa.
    # encoding='utf-8': Đảm bảo ghi được tiếng Việt có dấu.
    handler = TimedRotatingFileHandler(
        log_file_path,
        when='D',
        interval=1,
        backupCount=30,
        encoding='utf-8',
        delay=False
    )
    handler.setFormatter(log_format)
    handler.setLevel(logging.DEBUG) # Đặt mức log cho handler này

    # --- 5. (Tùy chọn) Thêm một handler để hiển thị log trên console ---
    # Giúp bạn dễ dàng debug khi đang phát triển
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)
    stream_handler.setLevel(logging.INFO) # Chỉ hiển thị log từ INFO trở lên trên console

    # --- 6. Thêm các handler vào logger ---
    # Tránh thêm handler nhiều lần nếu hàm này được gọi lại
    if not logger.handlers:
        logger.addHandler(handler)
        logger.addHandler(stream_handler)

    return logger


