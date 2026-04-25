# Dùng bản Python nhẹ nhất
FROM python:3.9-slim

# Tạo thư mục làm việc trong Container
WORKDIR /app

# Copy và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy file code vào
COPY watcher.py .

# Lệnh chạy mặc định
CMD ["python", "watcher.py"]