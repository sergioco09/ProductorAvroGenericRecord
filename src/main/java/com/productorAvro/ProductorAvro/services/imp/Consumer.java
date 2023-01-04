package com.productorAvro.ProductorAvro.services.imp;

//import com.productorAvro.ProductorAvro.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

//@SpringBootApplication
@Service
public class Consumer {

/*    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("group.id", "my-avro-consumer");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest");

        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        props.setProperty("specific.avro.reader", "true");
        //SpringApplication.run(ConsumidorAvroApplication.class, args);

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(props);
        String topic = "TP-AVRO-VENTAS";

        consumer.subscribe(Collections.singleton(topic));

        System.out.println("Recupera datos");

        while (true){
            ConsumerRecords<String, Customer> records = consumer.poll(500);
            for (ConsumerRecord<String, Customer> record : records){
                Customer customer = record.value();
                System.out.println("Escribe datos");
                System.out.println(customer);
            }
            consumer.commitSync();
        }

    }*/
}
