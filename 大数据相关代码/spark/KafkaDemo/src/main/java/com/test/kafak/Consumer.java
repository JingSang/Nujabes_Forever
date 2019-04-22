package com.test.kafak;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
    private static String TOPIC_NAME = "sentry-test";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf","D:\\javaProject\\kafkaDemo\\target\\classes\\krb5.conf");
        System.setProperty("java.security.auth.login.config","D:\\javaProject\\kafkaDemo\\target\\classes\\jaas-keytab.conf");
        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh4:9092,cdh5:9092,cdh6:9092");
        props.put("group.id", "songlei");
        props.put("auto.offset.reset", "latest");
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        ConsumerRecords<String, String> records = null;
        TopicPartition partition0= new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Arrays.asList(partition0));
        while (true) {
                System.out.println();
                records = consumer.poll(10000);

                for (ConsumerRecord<String, String> record : records) {

                    System.out.println("Receivedmessage: (" + record.key()
                            + "," + record.value() + ") at offset "
                            + record.offset());
                }
        }
    }
}

