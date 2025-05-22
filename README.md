#  Real-Time Gudang Monitoring System

**Nama:** Farand Febriansyah

**NRP:** 5027231084


##  Deskripsi Singkat

Proyek ini bertujuan untuk memantau kondisi gudang penyimpanan yang berisi barang-barang sensitif seperti makanan, obat-obatan, dan elektronik secara **real-time**. Sistem menggunakan **Apache Kafka** untuk menerima data dari sensor suhu dan kelembaban, dan **PySpark Structured Streaming** untuk mengolah dan menganalisis data.

---

##  Fitur Utama

*  **Simulasi Data Sensor** setiap detik untuk tiga gudang (`G1`, `G2`, `G3`)
*  **Peringatan Suhu Tinggi** jika suhu > 80°C
*  **Peringatan Kelembaban Tinggi** jika kelembaban > 70%
*  **Peringatan Kritis** jika kedua kondisi bahaya terjadi secara bersamaan
*  **Join Stream berdasarkan gudang\_id dan window waktu 10 detik**

---

##  Teknologi dan Komponen

### 1. **Apache Kafka**

Apache Kafka adalah platform streaming terdistribusi yang digunakan untuk mentransfer data secara real-time. Kafka menerima, menyimpan, dan mengirimkan data dalam bentuk *stream* menggunakan konsep *topics*.

* `sensor-suhu-gudang`: topik untuk data suhu
* `sensor-kelembaban-gudang`: topik untuk data kelembaban

### 2. **Apache ZooKeeper**

ZooKeeper bertugas sebagai *coordinator* untuk Kafka. Ia mengelola klaster Kafka dan menyimpan konfigurasi serta status broker Kafka.

### 3. **Kafka Producer**

Dua *producer* dibuat untuk mensimulasikan data sensor:

* `producer_suhu.py`: mengirim data suhu ke topik `sensor-suhu-gudang`
* `producer_kelembaban.py`: mengirim data kelembaban ke topik `sensor-kelembaban-gudang`

Contoh Format:

```json
{"gudang_id": "G1", "suhu": 82}
{"gudang_id": "G2", "kelembaban": 75}
```

### 4. **PySpark Structured Streaming**

Digunakan untuk membaca data secara langsung dari Kafka dan melakukan analisis real-time:

* Filtering suhu > 80°C dan kelembaban > 70%
* Join data suhu dan kelembaban berdasarkan `gudang_id` dan *time window*
* Menampilkan output status setiap gudang ke console

---

##  Arsitektur Sistem

```
[Sensor Suhu] ---> Kafka Topic: sensor-suhu-gudang
[Sensor Kelembaban] ---> Kafka Topic: sensor-kelembaban-gudang

    Kafka Broker (with Zookeeper)
                ↓
          PySpark Consumer
        [Streaming Analysis]
                |
                ↓
    [Output: Peringatan & Status]
```

---

##  Cara Menjalankan

### 1. Jalankan Apache Kafka dan ZooKeeper

```bash
# Jalankan ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Jalankan Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

![image](https://github.com/user-attachments/assets/2b4440a5-95f9-4021-b1c4-7d1461b0d921)


### 2. Buat Kafka Topics

```bash
bin/kafka-topics.sh --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

bin/kafka-topics.sh --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

![image](https://github.com/user-attachments/assets/092aceb3-e03b-4b0f-bfcc-5131a14ef31b)

### 3. Jalankan Producer

```bash
python producer_suhu.py
python producer_kelembaban.py
```

![image](https://github.com/user-attachments/assets/fc19af33-e252-41c5-8fd8-f5379cf38fff)

![image](https://github.com/user-attachments/assets/9d3358fc-70c8-4f40-b15c-c5941918c6db)

### 4. Jalankan PySpark Streaming

```bash
spark-submit consumer_streaming.py
```
![image](https://github.com/user-attachments/assets/6a38414a-1965-472c-b282-534ee9425b95)


##  Struktur Folder

```
project/
│
├── producer_suhu.py
├── producer_kelembaban.py
├── consumer_streaming.py
├── requirements.txt
└── README.md
```
