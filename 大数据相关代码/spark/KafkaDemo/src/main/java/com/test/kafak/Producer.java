package com.test.kafak;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Properties;

public class Producer {
    //发送topic
    public static String TOPIC_NAME = "sentry-test";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf","D:\\javaProject\\kafkaDemo\\target\\classes\\krb5.conf");
        System.setProperty("java.security.auth.login.config","D:\\javaProject\\kafkaDemo\\target\\classes\\jaas-keytab.conf");
        //配置参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh4:9092,cdh5:9092,cdh6:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("group.id", "songlei");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;

            String message = "Message-" + i;

            ProducerRecord record = new ProducerRecord<String, String>(
                    TOPIC_NAME, key, message);

            producer.send(record);

            System.out.println(key + "----" + message);

        }

        producer.close();

    }
}

