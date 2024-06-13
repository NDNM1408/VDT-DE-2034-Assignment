package com.viettel.vdt.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;



import org.apache.kafka.clients.producer.*;
import java.io.IOException;


public class Producer {
    private static final String TOPIC_NAME = "vdt2024";
    private static final String BOOTSTRAP_SERVERS = "localhost:9091,localhost:9092,localhost:9093";
    private static final String dataPath = "/home/minh/My project/VDT-2024-Assignment/data/log_action.csv";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
            String line;
            int recordCount = 0;
            ObjectMapper objectMapper = new ObjectMapper();
            while ((line = reader.readLine()) != null) {
                try {
                    Thread.sleep(1000); // Sleep for 1 second
                } catch (InterruptedException e) {
                    System.err.println("Interrupted while sleeping: " + e.getMessage());
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                }                // Process the CSV line and create a ProducerRecord
                String[] fields = line.split(",");
                int studentCode = Integer.parseInt(fields[0]);
                String activity = fields[1];
                int numberOfFiles = Integer.parseInt(fields[2]);
                String timeStamp = fields[3];
                LogAction logAction = new LogAction(studentCode, activity, numberOfFiles, timeStamp);
                String msg = objectMapper.writeValueAsString(logAction);
                // Send the record and handle the result with callback
                ProducerRecord<String, String> record = new ProducerRecord<String,String>(TOPIC_NAME, msg);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Record id " + record.key() + " sent successfully - topic: " + metadata.topic() +
                                    ", partition: " + metadata.partition() +
                                    ", offset: " + metadata.offset());
                        } else {
                            System.err.println("Error while sending record " + record.key() + ": " + exception.getMessage());
                        }
                    }
                });
            }

            System.out.println(recordCount + " records sent to Kafka successfully.");
        } catch (IOException e) {
            System.out.println(e);
        }
    }
    
    static class LogAction {
        private int student_code;
        private String activity;
        private int numberOfFile;
        private String timestamp;

        public LogAction(int student_code, String activity, int numberOfFile, String timestamp) {
            this.student_code = student_code;
            this.activity = activity;
            this.numberOfFile = numberOfFile;
            this.timestamp = timestamp;
        }

        public int getStudent_code() {
            return student_code;
        }

        public void setStudent_code(int student_code) {
            this.student_code = student_code;
        }

        public String getActivity() {
            return activity;
        }

        public void setActivity(String activity) {
            this.activity = activity;
        }

        public int getNumberOfFile() {
            return numberOfFile;
        }

        public void setNumberOfFile(int numberOfFile) {
            this.numberOfFile = numberOfFile;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }
    }
}