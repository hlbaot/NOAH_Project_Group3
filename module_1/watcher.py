import os
import time
import shutil
import csv
import mysql.connector
from datetime import datetime

# --- ĐƯỜNG DẪN TRONG DOCKER---
INPUT_DIR = '/app/input'
PROCESSED_DIR = '/app/processed'
ERROR_DIR = '/app/error'

def connect_db():
    # Khi chạy chung docker-compose, host chính là tên của service MySQL
    return mysql.connector.connect(
        host="mysql", 
        user="root",
        password="rootpassword",
        database="webstore"
    )

def process_file(filepath):
    filename = os.path.basename(filepath)
    
    valid_count = 0
    skipped_count = 0

    try:
        db = connect_db()
        cursor = db.cursor()

        with open(filepath, 'r', encoding='utf-8') as f:
            # file phân cách bằng dấu phẩy
            reader = csv.DictReader(f, delimiter=',')
            
            for row in reader:
                try:
                    # Lấy dữ liệu từ cột 
                    raw_id = row['product_id']
                    raw_qty = row['quantity']

                    # XỬ LÝ LỖI DIRTY DATA 
                    product_id = int(float(raw_id))
                    stock_qty = int(float(raw_qty))

                    # Kiểm tra số lượng âm
                    if stock_qty < 0:
                        raise ValueError(f"Số lượng bị âm ({stock_qty})")

                    # CẬP NHẬT DATABASE 
                    sql = "UPDATE products SET stock = %s WHERE id = %s"
                    cursor.execute(sql, (stock_qty, product_id))
                    valid_count += 1

                except Exception as e:
                    skipped_count += 1
                    print(f"  -> [BỎ QUA] Dòng lỗi {row}: {e}")

        # Lưu thay đổi vào DB
        db.commit()
        cursor.close()
        db.close()

        # DỌN DẸP FILE 
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"done_{timestamp}_{filename}"
        shutil.move(filepath, os.path.join(PROCESSED_DIR, new_filename))
        
        print(f"[INFO] Processed {valid_count} records. Skipped {skipped_count} invalid records.")

    except Exception as e:
        print(f"LỖI: {e}")
        shutil.move(filepath, os.path.join(ERROR_DIR, filename))

def start_watching():
    while True:
        # Polling: Quét xem có file không
        files = os.listdir(INPUT_DIR)
        for file in files:
            if file.endswith('.csv'):
                full_path = os.path.join(INPUT_DIR, file)
                process_file(full_path)
        
        #Ngủ 5 giây rồi quét tiếp
        time.sleep(5)

if __name__ == "__main__":
    start_watching()