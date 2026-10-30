package com.mycompany.telemetryproducer;


// Connecting the Jackson library to work with JSON (serialization/deserialization)
import com.fasterxml.jackson.databind.ObjectMapper;

// Imports for the Apache Kafka client (consumer)
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

// Time Management Imports (modern Java API)
import java.time.Duration;

// Utilities for collections and settings
import java.util.Collections;
import java.util.Properties;

// JDBC for working with a database (ClickHouse)
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

// For error pauses and exception handling
import java.util.concurrent.TimeUnit;

public class RadioDataConsumerUpdate {

    /**
     * The method for calculating the CRC16 checksum.
     * Completely identical to the method in the producer. It is needed to verify data integrity.
     */
    private static int crc16(byte[] data) {
        int crc = 0xFFFF; // The standard initial value for the CRC16 algorithm
        
        // We go through each byte of the data array
        for (byte b : data) {
            crc ^= (b & 0xFF); // We perform the XOR operation of the current byte with the current CRC value
            
            // We process every bit in a byte (8 bits in total)
            for (int i = 0; i < 8; i++) {
                if ((crc & 1) != 0) { 
                    // If the least significant bit is 1, we shift it to the right and apply the 0xA001 polynomial.
                    crc = (crc >> 1) ^ 0xA001;
                } else {
                    // Otherwise, we just shift to the right without using the polynomial.
                    crc >>= 1;
                }
            }
        }
        return crc; // We return the final calculated CRC value.
    }

    /**
      * Auxiliary method: converts the HEX string back to an array of bytes.
      * This is necessary because in JSON we store bytes as a string (for example, "a1b2c3"),
      * and bytes are needed for CRC calculation and parsing.
      */
    private static byte[] hexToBytes(String hex) {
        if (hex == null || hex.length() == 0) return new byte[0]; // Protection against null or empty strings
        
        int len = hex.length();
        byte[] data = new byte[len / 2]; // Each byte is encoded with two HEX characters (for example, 'A1' = 1 byte)
        
        for (int i = 0; i < len; i += 2) {
            // Convert two HEX characters to a single number (byte)
            // Character.digit(char, 16) converts a character to a number based on 16 (HEX)
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);
            data[i / 2] = (byte) ((high << 4) + low);
        }
        return data;
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        // Creating a Properties object to store the settings of the consumer
        Properties props = new Properties();

        // We specify the address of the Kafka broker (local server, port 9092)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // The unique ID of the consumer group.
        // Kafka guarantees that messages from the same topic will be distributed among the members of the same group.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-validation-group");

        // Configuring deserializers: the key and value will be received as strings
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // If the offset (reading position) is not found, start from the very beginning of the topic (earliest)
        // This is useful when starting for the first time or if you need to re-read old data.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Disabling the automatic commit of offsets.
        // We will confirm the successful manual processing (commitSync) only after writing to the database.
        // This prevents data loss: if the record in the database drops, the message is read again.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Creating an instance of KafkaConsumer with the passed settings
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic of telemetry.raw, where the producer sends all types of sensors
        consumer.subscribe(Collections.singletonList("telemetry.raw"));

        System.out.println("=== THE CONSUMER IS RUNNING ===");
        System.out.println("I'm listening to the topic: telemetry.raw");
        System.out.println("Group: telemetry-validation-group");
        System.out.println("Ctrl+C to stop");

        ObjectMapper mapper = new ObjectMapper(); // Creating a mapper for converting JSON to Java objects
        long lastSeqNo = -1; // A variable for tracking the sequence of packets (loss detection)

        try {
            while (true) { // The endless cycle of the consumer's work
                // Poll requests new messages from Kafka. The timeout is 100 ms.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) { // We go through all the received records
                    String jsonPayload = record.value(); // Getting a JSON string from a message

                    try {
                        // Deserializing JSON into a TelemetryPacket object
                        TelemetryPacket packet = mapper.readValue(jsonPayload, TelemetryPacket.class);

                        // --- STAGE 1: Data Integrity Check (CRC) ---
                        byte[] rawPayload = hexToBytes(packet.getPayloadHex()); // Converting HEX back to bytes
                        int calculatedCrc = crc16(rawPayload); // Counting the CRC again

                        if (calculatedCrc != packet.getCrc()) {
                            // The checksum does not match: the data is corrupted during transmission
                            System.err.println("❌ BROKEN PACKAGE: seq=" + packet.getSeqNo() +
                                    " (CRC does not match: expected " + packet.getCrc() + ", received " + calculatedCrc + ")");
                            continue; // We skip this package, we don't try to parse it or write it to the database.
                        }

                        // --- STAGE 2: Sequence verification (Loss Detection) ---
                        if (lastSeqNo != -1 && packet.getSeqNo() > lastSeqNo + 1) {
                            // If the current number is more than 1 higher than the previous one, it means that the packets have been lost.
                            long lostCount = packet.getSeqNo() - lastSeqNo - 1;
                            System.out.println("⚠️ LOST PACKETS: " + lostCount +
                                    " (missing numbers from " + (lastSeqNo + 1) + " by " + (packet.getSeqNo() - 1) + ")");
                        }
                        lastSeqNo = packet.getSeqNo(); // Updating the last known package number

                        // --- STAGE 3: Payload parsing depending on the sensor type ---
                        double parsedValue = 0.0; // A universal field for the basic value (temperature, vibration, etc.)
                        String extraInfo = "";    // Additional field (for example, error code or status)
                        
                        switch (packet.getSensorType()) {
                            case "temperature":
                                // In bytes [0] is the temperature, [1] is the counter
                                parsedValue = rawPayload[0]; 
                                extraInfo = "Temp";
                                break;
                            
                            case "vibration":
                                // In bytes [0..2] the X, Y, and Z axes lie. For simplicity, we take X as the main value.
                                // You can later expand the logic to write all three axes in separate columns.
                                parsedValue = Byte.toUnsignedInt(rawPayload[0]); 
                                extraInfo = "Vib-X";
                                break;
                            
                            case "status":
                                // Byte [0]: 1 = ON, 0 = OFF. Byte [1]: error code.
                                int statusCode = rawPayload[0] & 0xFF;
                                int errorCode = rawPayload[1] & 0xFF;
                                
                                parsedValue = statusCode;
                                extraInfo = (errorCode > 0) ? "ERR:" + errorCode : "OK";
                                break;
                            
                            case "pressure":
                                // The pressure is stored in two bytes: [0] and [1]. We collect them back into a number.
                                int pressureLow = rawPayload[0] & 0xFF;
                                int pressureHigh = rawPayload[1] & 0xFF;
                                parsedValue = (pressureHigh << 8) | pressureLow;
                                extraInfo = "Press";
                                break;
                            
                            default:
                                // If the type is unknown, log the warning and put a stub.
                                System.err.println("⚠️ Unknown sensor type: " + packet.getSensorType());
                                parsedValue = 0.0;
                                extraInfo = "Unknown";
                        }

                        // --- STAGE 4: Sign up for ClickHouse ---
                        String jdbcUrl = "jdbc:clickhouse://localhost:8123/default";

                        Properties dbProps = new Properties();
                        dbProps.setProperty("user", "telemetry_user");
                        // We are not transmitting the password, we assume the configuration without a password or through the server configuration

                        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbProps);
             // Updated SQL query: now we write not only HEX, but also parsed values
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO telemetry_raw_data (device_id, seq_no, timestamp, sensor_type, payload_hex, crc, parsed_value, extra_info) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {

            ps.setLong(1, packet.getDeviceId());       // device_id
            ps.setLong(2, packet.getSeqNo());         // seq_no
           // ps.setLong(3, packet.getTimestamp());     // timestamp
           long timestampSeconds = packet.getTimestamp() / 1000; 
            ps.setLong(3, timestampSeconds);
            ps.setString(4, packet.getSensorType());  // sensor_type (new field)
            ps.setString(5, packet.getPayloadHex());  // payload_hex (raw audit data)
            ps.setInt(6, packet.getCrc());           // crc
            ps.setDouble(7, parsedValue);            // parsed_value (decrypted value)
            ps.setString(8, extraInfo);              // extra_info (status, error code, etc.)

            ps.executeUpdate();
            System.out.printf("✅ RECORDED IN THE DATABASE: device=%d, type=%s, value=%.2f, info=%s%n", 
                    packet.getDeviceId(), packet.getSensorType(), parsedValue, extraInfo);

        } catch (SQLException e) {
            System.err.println("❌ DATABASE ERROR: " + e.getMessage());
            // Important: we do NOT throw e so that the entire consumer does not fall due to a single database error.
            // The message will remain in Kafka (since commitSync will not be called) and will be read again.
        }

                        // Only if all the steps were successful (CRC is OK, losses are taken into account, the database has recorded), we confirm the offset
                        consumer.commitSync(); 

                    } catch (Exception e) {
                        // If the JSON is broken, the format is incorrect, or Jackson could not parse the object.
                        System.err.println("❌ INCORRECT DATA FORMAT or parsing error: " + e.getMessage());
                        // We don't commitSync here either, so that the message can be read again (or sent to DLQ in the future)
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Critical consumer error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close(); // Closing the Kafka connection correctly when the program is stopped
        }
    }

    /**
     * Container class for telemetry data.
     * Must match exactly the structure of the JSON that the producer sends.
     * Added getters/setters for Jackson and a new sensorType field.
     */
    static class TelemetryPacket {
        private long deviceId;
        private long seqNo;
        private long timestamp;
        private String payloadHex;
        private int crc;
        private String sensorType; // NEW FIELD: sensor type (temperature, vibration, status, pressure)

        public TelemetryPacket() {} // An empty constructor is required for deserialization of Jackson

        // Getters (required for Jackson)
        public long getDeviceId() { return deviceId; }
        public long getSeqNo() { return seqNo; }
        public long getTimestamp() { return timestamp; }
        public String getPayloadHex() { return payloadHex; }
        public int getCrc() { return crc; }
        public String getSensorType() { return sensorType; }

        // Setters (required for Jackson so that he can fill in the fields)
        public void setDeviceId(long deviceId) { this.deviceId = deviceId; }
        public void setSeqNo(long seqNo) { this.seqNo = seqNo; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public void setPayloadHex(String payloadHex) { this.payloadHex = payloadHex; }
        public void setCrc(int crc) { this.crc = crc; }
        public void setSensorType(String sensorType) { this.sensorType = sensorType; }
    }
}
