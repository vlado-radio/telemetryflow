package com.mycompany.telemetryproducer;

// We’re including the Jackson library: it can convert Java objects into JSON and back again.
import com.fasterxml.jackson.databind.ObjectMapper;

// We are connecting the Apache Kafka client to send data.
import org.apache.kafka.clients.producer.*;

// They are needed for handling exceptions, working with user input, and multithreading.
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import com.fasterxml.jackson.core.JsonProcessingException;

public class RadioDataProducerUpdate {


// The CRC16 checksum calculation method

    private static int crc16(byte[] data) {
        int crc = 0xFFFF; // The initial CRC value (standard for this algorithm)
        
        // We go through every byte of data.
        for (byte b : data) {
            crc ^= (b & 0xFF); // XOR (exclusive OR) of the current byte with the CRC
            
            // 8 steps for each bit in a byte
            for (int i = 0; i < 8; i++) {
                if ((crc & 1) != 0) { 
                    // If the least significant bit is 1, shift right and apply the polynomial (0xA001).
                    crc = (crc >> 1) ^ 0xA001;
                } else {
                    // Otherwise, we just shift it to the right.
                    crc >>= 1;
                }
            }
        }
        return crc; // We return the final number (the checksum).
    }

    // Entry point to the program.
    public static void main(String[] args) throws InterruptedException, ExecutionException, JsonProcessingException {
        
        // We create a settings (property) object for our producer (sender).
        Properties props = new Properties();
        
        // We’re telling you where to find Kafka: on the same computer, port 9092.
        props.put("bootstrap.servers", "localhost:9092");
        
        // Kafka stores data in pairs: Key and Value.
        // Here we say: "Both the key and the value will be strings"
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // acks="1" means: “wait for confirmation only from the partition leader, do not wait for a replica.”
        // This is a balance between speed and reliability. For telemetry, speed is more important.
        props.put("acks", "1"); 

        // We create the sender object itself, passing the settings to it.
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // We create a mapper (converter) for working with JSON.
        ObjectMapper mapper = new ObjectMapper();

        // --- USER DATA INPUT BLOCK ---
        Scanner scanner = new Scanner(System.in);
        System.out.println("=== RADIO TRANSMITTER EMULATOR SETUP ===");
        System.out.print("Enter the number of sensors to create:");
        int totalSensors = scanner.nextInt();
        scanner.nextLine(); // We read the rest of the string after the number.

        List<SensorConfig> configs = new ArrayList<>();

        for (int i = 1; i <= totalSensors; i++) {
            System.out.printf("--- Sensor setup №%d ---%n", i);
            System.out.print("Select the type of sensor (temperature / vibration / status / pressure): ");
            String type = scanner.nextLine().trim().toLowerCase();
            
            // Type validation if the user made a typo
            while (!Arrays.asList("temperature", "vibration", "status", "pressure").contains(type)) {
                System.out.print("Invalid type. Enter temperature, vibration, status, or pressure: ");
                type = scanner.nextLine().trim().toLowerCase();
            }

            // Для событийного типа (status) частота не так важна, но оставим её для единообразия логики потока
            long frequencyMs;
            if (type.equals("status")) {
                System.out.println("For the ‘status’ type, the frequency is ignored, and events are generated randomly.");
                frequencyMs = 5000; // The plug is not actually used in a loop of this type.
            } else {
                System.out.print("Sending frequency (in milliseconds, e.g., 1000 = 1 sec): ");
                frequencyMs = scanner.nextLong();
                scanner.nextLine();
            }

            configs.add(new SensorConfig(i, type, frequencyMs));
        }
        scanner.close();
        // -----------------------------------------

        System.out.println("\n=== SIMULATION START ===");
        System.out.println("Send to the topic: telemetry.raw");
        System.out.println("Ctrl+C to stop the entire program");
        System.out.println("-------------------------------------");

        // We start a separate thread for each sensor.
        // This allows them to work in parallel at different frequencies.
        for (SensorConfig config : configs) {
            Thread thread = new Thread(() -> {
                try {
                    runSensorLoop(config, producer, mapper);
                } catch (Exception e) {
                    System.err.println("Error in the sensor stream " + config.getId() + ": " + e.getMessage());
                }
            });
            thread.setName("Sensor-" + config.getType());
            thread.start();
        }
    }

    /**
     * The main operating cycle of one specific sensor.
     * Each sensor has its own ID, type, and polling frequency.
     */
    private static void runSensorLoop(SensorConfig config, KafkaProducer<String, String> producer, ObjectMapper mapper) 
            throws InterruptedException, JsonProcessingException, ExecutionException {
        
        long deviceId = config.getId();
        String sensorType = config.getType();
        long intervalMs = config.getIntervalMs();
        long seqNo = 0L;
        Random random = new Random(); // Random number generator for realistic data

        while (true) {
            // We generate data depending on the type of sensor.
            byte[] rawPayload = generatePayload(sensorType, random, seqNo);

            // We calculate the checksum for this data.
            int crcValue = crc16(rawPayload);

            // We gather everything into a single object‑package (a wrapper around the data).
            TelemetryPacket packet = new TelemetryPacket(deviceId, seqNo, System.currentTimeMillis(), rawPayload, crcValue, sensorType);

            // We convert a Java object into a JSON string.
            String jsonPayload = mapper.writeValueAsString(packet);

            // Preparing the record for sending:
            // Topic: "telemetry.raw"
            // Key: String.valueOf(deviceId) — this is needed so that Kafka knows which partition to put the data into.
            // IMPORTANT: Packets with the same device_id always go to the same partition and strictly in order.
            ProducerRecord<String, String> record = new ProducerRecord<>("telemetry.raw", String.valueOf(deviceId), jsonPayload);
            
            // We are sending data to Kafka.
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // If Kafka returned an error (the network failed, the broker died), write it to the log in red.
                    System.err.println("❌ Sensor " + deviceId + " (type " + sensorType + ") Error sending: " + exception.getMessage());
                } else {
                    // If everything is fine, we write to the console: which packet number was sent and what its offset is
                    // We use synchronized to prevent messages from different threads from getting mixed up in the console
                    synchronized (System.out) {
                        System.out.printf("[%s-%d] ✅ Sent: seq=%d, offset=%d%n", sensorType, deviceId, packet.getSeqNo(), metadata.offset());
                    }
                }
            });

            seqNo++; // We increase the package counter for the next iteration.
            
            // We determine the pause before the next message.
            long sleepTime = calculateSleepTime(sensorType, intervalMs, random);
            Thread.sleep(sleepTime);
        }
    }

    /**
     * Generates “raw” bytes depending on the sensor type.
     * Instead of random bytes, we now generate meaningful data, 
     * which will then be interpreted in the consumer.
     */
    private static byte[] generatePayload(String type, Random random, long seqNo) {
        byte[] payload = new byte[16]; // We slightly increased the size to diversify the data.

        switch (type) {
            case "temperature":
                // Temperature simulation: basic level + slight noise
                int tempBase = 25; 
                int noise = random.nextInt(5) - 2; // Noise from -2 to +2 degrees.
                int temperature = tempBase + noise;
                // We encode the temperature into bytes (simplified: we simply write the number in the first bytes).
                payload[0] = (byte) temperature; 
                payload[1] = (byte) (seqNo % 256); // Let’s add a counter for uniqueness.
                break;

            case "vibration":
                // Vibration simulation: 3 axes (X, Y, Z) with random values
                byte x = (byte) (random.nextInt(100) - 50);
                byte y = (byte) (random.nextInt(100) - 50);
                byte z = (byte) (random.nextInt(100) - 50);
                payload[0] = x;
                payload[1] = y;
                payload[2] = z;
                payload[3] = (byte) (seqNo % 256);
                break;

            case "status":
                // Device status: 1 = ON, 0 = OFF. 
                // Generate a random event (event-driven model)
                // Probability of state change: 20% per cycle
                boolean isOn = random.nextDouble() < 0.2; 
                payload[0] = isOn ? (byte) 1 : (byte) 0;
                
                // Let’s add an error code (0 = OK, 1–255 = error).
                int errorCode = random.nextInt(10) == 0 ? random.nextInt(255) : 0;
                payload[1] = (byte) errorCode;
                break;

            case "pressure":
                // Pressure: base value + drift + noise
                int basePressure = 1013;
                int drift = (int) (seqNo / 100); // A slow drift over time
                int pressureNoise = random.nextInt(10);
                int pressure = basePressure + drift + pressureNoise;
                
                // We write the pressure to bytes (just the lower bytes, for example).
                payload[0] = (byte) (pressure & 0xFF);
                payload[1] = (byte) ((pressure >> 8) & 0xFF);
                break;

            default:
                // If the type is unknown, we generate garbage (fallback).
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = (byte) random.nextInt(256);
                }
        }
        return payload;
    }

    /**
     * Calculates the delay time (sleep) in milliseconds.
     * For the event type (status), the delay is random to simulate irregular events.
     */
    private static long calculateSleepTime(String type, long baseInterval, Random random) {
        if (type.equals("status")) {
            // Status events occur randomly: from 1 to 10 seconds.
            return 1000 + random.nextInt(9000);
        } else {
            // For the rest, we add a small amount of jitter (time jitter) so that the data doesn’t arrive perfectly synchronously
            // Jitter +/- 20% of the interval
            long jitter = (long) (baseInterval * 0.2 * (random.nextDouble() * 2 - 1));
            return Math.max(100, baseInterval + jitter);
        }
    }

    /**
     * A helper container class for configuring a single sensor.
     * Stores the ID, type, and polling interval.
     */
    static class SensorConfig {
        private final long id;
        private final String type;
        private final long intervalMs;

        public SensorConfig(long id, String type, long intervalMs) {
            this.id = id;
            this.type = type;
            this.intervalMs = intervalMs;
        }

        public long getId() { return id; }
        public String getType() { return type; }
        public long getIntervalMs() { return intervalMs; }
    }

    /**
     * Auxiliary container class.
     * It simply stores the package data.
     * The sensorType field has been added so that the consumer can immediately understand what kind of data has been received.
     */
    static class TelemetryPacket {
        private long deviceId;
        private long seqNo;
        private long timestamp;
        private String payloadHex; // We store bytes as a HEX string because JSON cannot store “raw” bytes.
        private int crc;
        private String sensorType; // New field: sensor type

        // Constructor: accepts all data and immediately converts bytes to HEX.
        public TelemetryPacket(long deviceId, long seqNo, long timestamp, byte[] payload, int crc, String sensorType) {
            this.deviceId = deviceId;
            this.seqNo = seqNo;
            this.timestamp = timestamp;
            this.payloadHex = bytesToHex(payload); // Calling the conversion method
            this.crc = crc;
            this.sensorType = sensorType;
        }

        // Getters (methods for retrieving data).
        // They are REQUIRED for the Jackson library.
        public long getDeviceId() { return deviceId; }
        public long getSeqNo() { return seqNo; }
        public long getTimestamp() { return timestamp; }
        public String getPayloadHex() { return payloadHex; }
        public int getCrc() { return crc; }
        public String getSensorType() { return sensorType; }
    }

    /**
     * Converts an array of bytes to a HEX string.
     * Example: the byte [10, 255] will be converted to the string "0aff".
     * This is needed to prevent JSON from breaking on special characters.
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2); // Reserve space in memory in advance
        for (byte b : bytes) {
            // %02x means: output the number in hexadecimal form, at least 2 characters, pad with a zero on the left
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
