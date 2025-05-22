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
            suhu = random.randint(75, 90)  # suhu antara 75-90
            data = {"gudang_id": gudang, "suhu": suhu}
            producer.send('sensor-suhu-gudang', value=data)
            print(f"[Producer Suhu] Mengirim data: {data}")
            time.sleep(1)
except KeyboardInterrupt:
    print("Producer suhu dihentikan.")
finally:
    producer.close()
