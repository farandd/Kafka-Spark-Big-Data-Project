import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

try:
    while True:
        for gudang in gudangs:
            kelembaban = random.randint(60, 80)  # kelembaban antara 60-80%
            data = {"gudang_id": gudang, "kelembaban": kelembaban}
            producer.send('sensor-kelembaban-gudang', value=data)
            print(f"[Producer Kelembaban] Mengirim data: {data}")
            time.sleep(1)
except KeyboardInterrupt:
    print("Producer kelembaban dihentikan.")
finally:
    producer.close()
