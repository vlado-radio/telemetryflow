# Telemetry Flow

An end‑to‑end simulation and ingestion pipeline for radio sensor telemetry: from mock data generation through Apache Kafka to validated storage in ClickHouse.

This project demonstrates a real‑world streaming data flow with integrity checks, loss detection, and type‑aware parsing—ideal for telecom, automation, and radio communications systems.

## 🎯 Core Features

- **Realistic sensor simulation**: supports temperature, vibration, pressure, and status sensors with distinct data models, noise, and interval variations.
- **Data integrity**: CRC16 checksum calculation and verification on both producer and consumer sides.
- **Packet loss detection**: sequence number tracking to identify missing messages.
- **Type‑aware parsing**: automatic extraction of meaningful values (e.g., 3‑axis vibration, error codes) from raw bytes.
- **Reliable ingestion**: manual offset commit only after successful database write (at‑least‑once guarantee, no data loss).
- **Multi‑threaded producer**: each sensor runs in its own thread with independent frequency.
- **Interactive setup**: CLI wizard to configure the number of sensors, their types, and sending frequencies.
- **Convenient macOS launch**: the `run-both.sh` script automatically starts the consumer, then the producer, and gracefully stops both processes on exit.

---

## 🧠 Architecture Overview

1. **Producer (`RadioDataProducerUpdate.java`)**:
   - Generates mock telemetry packets with realistic noise, drift, and event probabilities.
   - Calculates CRC16 checksums and encodes raw bytes as HEX strings for safe JSON transport.
   - Sends data to the Kafka topic `telemetry.raw`, using `device_id` as the key (ensures ordered delivery per device).
   - Uses `acks=1` to balance speed and reliability.

2. **Consumer (`RadioDataConsumerUpdate.java`)**:
   - Subscribes to `telemetry.raw` and processes messages in a loop with a 100 ms poll timeout.
   - Validates CRC and detects packet losses via sequence numbers.
   - Parses payload based on sensor type and extracts structured values.
   - Inserts data into the ClickHouse table `telemetry_raw_data`, storing both raw and parsed fields.
   - Commits offsets only after successful DB write (at‑least‑once semantics).

3. **Data Flow**:  
   `Sensors → Producer (Java + Kafka) → Kafka Topic → Consumer (Java + Kafka + JDBC) → ClickHouse`

---

## ⚙️ Tech Stack

- **Language**: Java 11+
- **Streaming**: Apache Kafka (Producer & Consumer APIs)
- **Serialization**: Jackson (JSON)
- **Database**: ClickHouse (via JDBC)
- **Build & Launch**: Apache Maven
- **Algorithms**: CRC16 (custom implementation), HEX encoding/decoding
- **Concurrency**: Java Threads, synchronized I/O

---

## 🚀 Quick Start (macOS)

### Prerequisites

- JDK 11 or higher
- Apache Kafka running on `localhost:9092`
- ClickHouse running on `localhost:8123` with database `default`
- A ClickHouse user `telemetry_user` (no password required for demo; adjust as needed)
- Apache Maven (the project includes a bundled version in `apache-maven-3.10.0` for reproducible local runs)

### Database Setup

Create the target table in ClickHouse:

```sql
CREATE TABLE IF NOT EXISTS telemetry_raw_data
(
    device_id      UInt64,
    seq_no         UInt64,
    timestamp      DateTime64(3),
    sensor_type    LowCardinality(String),R
    payload_hex    String,
    crc            UInt16,
    parsed_value   Float64,
    extra_info     String
)
ENGINE = MergeTree()
ORDER BY (device_id, timestamp);
```
## 🚀 Launch via Script (Recommended for macOS)

The root directory contains `run-both.sh`, which:

- Starts the consumer in the background.
- Waits 3 seconds for initialization.
- Starts the producer (with interactive input).
- On producer exit, gracefully stops the consumer.

Run:

```bash
chmod u+x run-both.sh
./run-both.sh
```
During execution, enter sensor configuration data in the console. The script manages the lifecycle of both processes.

## 🛠️ Alternative Manual Launch

If you prefer to run components separately:

1. Ensure dependencies are configured (Maven/Gradle) for:
   - `org.apache.kafka:kafka-clients`
   - `com.fasterxml.jackson.core:jackson-databind`
   - `clickhouse-jdbc`
2. Compile the Java classes.
3. Start the producer:

```bash
./apache-maven-3.10.0/bin/mvn exec:java -Dexec.mainClass="com.mycompany.telemetryproducer.RadioDataProducerUpdate"
```
Follow the CLI prompts to configure sensors.

4. In another terminal, start the consumer:

```bash
./apache-maven-3.10.0/bin/mvn exec:java -Dexec.mainClass="com.mycompany.telemetryproducer.RadioDataConsumerUpdate"
```
Monitor logs for sent/received packets and DB inserts. Stop with `Ctrl+C`.

---

## 📊 Sample Data Model

| Field | Type | Description |
| --- | --- | --- |
| `device_id` | Int64 | Unique sensor identifier |
| `seq_no` | Int64 | Monotonic sequence number (per device) |
| `timestamp` | Int64 | Unix timestamp (seconds) |
| `sensor_type` | String | e.g., `temperature`, `vibration` |
| `payload_hex` | String | Raw bytes encoded as HEX |
| `crc` | Int32 | CRC16 checksum from producer |
| `parsed_value` | Float64 | Extracted numeric value |
| `extra_info` | String | Contextual info (e.g., error codes) |

---

## 🔍 Key Design Decisions

- **Ordered delivery**: using `device_id` as the Kafka key ensures all messages for a single sensor go to the same partition and are consumed in order.
- **At‑least‑once delivery**: manual offset commit after DB write prevents data loss even if the consumer crashes mid‑process.
- **HEX encoding**: raw byte arrays are converted to HEX strings to safely transport binary data in JSON without corruption.
- **Jitter in producer**: adds ±20% randomness to intervals to simulate real‑world network variability.
- **Event‑driven status sensors**: random event generation instead of fixed intervals.
- **Automated launch**: the `run-both.sh` script makes system startup and shutdown predictable and convenient, especially for frequent development iterations.

---

## 📁 Project Structure

```text
src/
├── main/
│   └── java/com/mycompany/telemetryproducer/
│       ├── RadioDataProducerUpdate.java
│       └── RadioDataConsumerUpdate.java
run-both.sh                  # Script for convenient macOS launch
apache-maven-3.10.0/         # Bundled Maven version (for local, reproducible runs)
```
## 📝 Notes & Limitations

- This is a **simulation/demo project**, not production‑ready. For production use, consider:
  - Secure credential management (avoid hardcoded DB properties).
  - Dead Letter Queue (DLQ) for unprocessable or invalid messages.
  - Connection pooling and retry logic for ClickHouse.
  - Metrics and logging (e.g., Micrometer, SLF4J).
- The consumer is designed for a single instance for simplicity. For scaling, use multiple consumers in the same group.
- CRC validation and sequence tracking are implemented manually for educational clarity; in production, evaluate built‑in mechanisms or additional validation layers.

---

## 🤝 Contribution

Contributions, bug reports, and feature ideas are welcome! Please open an issue or submit a pull request.

## 📄 License

MIT License — see the `LICENSE` file for details.

---

## 💡 Tips for `run-both.sh`

- **Execution permissions**: always run `chmod +x run-both.sh` before launching—macOS won’t execute it otherwise.
- **Maven path**: the script uses a local Maven version (`apache-maven-3.10.0`) for reproducible runs. If you have Maven installed system‑wide, you can replace the paths with just `mvn`.
- **Graceful shutdown**: the script cleanly stops the consumer when the producer exits (via `kill` and `wait`), preventing orphaned processes.
- **Terminal colors**: the colored output (red/green) works in the standard macOS terminal and helps quickly see launch status.

