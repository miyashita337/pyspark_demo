# writer.py
import csv, time, random, os
from datetime import datetime

os.makedirs("stream/in", exist_ok=True)
categories = ["Electronics","Home","Grocery","Sports","Books"]

i = 0
while True:
    path = f"stream/in/batch_{int(time.time())}.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        # ヘッダ
        w.writerow(["event_time","category","price","qty"])
        # 1バッチに数行だけ入れる
        for _ in range(random.randint(2,5)):
            w.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(categories),
                round(random.uniform(5, 1500), 2),
                random.randint(1, 5),
            ])
    i += 1
    time.sleep(2)

