package org.example;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.example.data.Ride;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

public class JsonProducer {
    private Properties props = new Properties();
    public JsonProducer() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-ewzgj.europe-west4.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }
    public List<Ride> getRides() throws IOException {
        Path filePath = Paths.get("/workspaces/data-engineering-camp2024/kafka_examples/src/main/resources/rides.csv");
        
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            return reader.lines()
                .skip(1) // Skip the header row
                .map(line -> new Ride(line.split(","))) // Assuming Ride has a constructor that takes a String array
                .collect(Collectors.toList());
        }
    }
    public static List<String[]> readCSV(Path filePath) throws IOException {
        List<String[]> records = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(values);
            }
        }
        return records;
    }
    // public void publishRides(List<Ride> rides) throws ExecutionException, InterruptedException {
    //     KafkaProducer<String, Ride> kafkaProducer = new KafkaProducer<>(props);
    //     for(Ride ride: rides) {
    //         ride.tpep_pickup_datetime = LocalDateTime.now().minusMinutes(20);
    //         ride.tpep_dropoff_datetime = LocalDateTime.now();
    //         var record = kafkaProducer.send(new ProducerRecord<>("rides", String.valueOf(ride.DOLocationID), ride), (metadata, exception) -> {
    //             if(exception != null) {
    //                 System.out.println(exception.getMessage());
    //             }
    //         });
    //         System.out.println(record.get().offset());
    //         System.out.println(ride.DOLocationID);
    //         Thread.sleep(500);
    //     }
    // }
    public void publishRides(List<Ride> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, Ride> kafkaProducer = new KafkaProducer<>(props);
        for(Ride ride: rides) {
            var record = kafkaProducer.send(new ProducerRecord<>("rides", String.valueOf(ride.DOLocationID), ride));
            // System.out.println(record.get().offset());
            // System.out.println(ride.DOLocationID);
        }
        kafkaProducer.close();
    }
    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new JsonProducer();
        var rides = producer.getRides();
        producer.publishRides(rides);
    }
}