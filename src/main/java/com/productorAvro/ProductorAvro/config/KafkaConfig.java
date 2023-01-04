package com.productorAvro.ProductorAvro.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("localhost_9092")
    String BOOTSTRAP_SERVERS;

    @Value("${acks.config}")
    String ACKS_CONFIG;

    @Value("${retries.config}")
    String RETRIES_CONFIG;

    @Value("${key.serializer.config}")
    String KEY_SERIALIZER_CONFIG;

    @Value("${value.serializer.config}")
    String VALUE_SERIALIZER_CONFIG;


    @Bean(name = "PropetiesKafkaSend")
    public Properties PublicadorConfig() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("acks", ACKS_CONFIG);
        props.put("retries.config", RETRIES_CONFIG);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://127.0.0.1:8081");
        return props;
    }
}
