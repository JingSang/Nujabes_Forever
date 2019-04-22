package com.test.kafak;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class MyProperties extends Properties{
    private Properties properties;

    private static final String JAAS_TEMPLATE =
            "KafkaClient {\n"
                    + "com.sun.security.auth.module.Krb5LoginModule required\n" +
                    "useKeyTab=true\n" +
                    "keyTab=\"%1$s\"\n" +
                    "principal=\"%2$s\";\n"
                    + "};";


    public MyProperties(){
        properties = new Properties();
    }

    public MyProperties self(){
        return this;
    }

    public MyProperties put(String key , String value) {
        if (properties == null) {
            properties = new Properties();
        }

        properties.put(key, value);
        return self();
    }

    public static MyProperties initKerberos(){
        return new MyProperties()
                .put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer")
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
                .put("security.protocol", "SASL_PLAINTEXT")
                .put("sasl.kerberos.service.name", "kafka");

    }

    public static MyProperties initProducer(){
        return new MyProperties()
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .put("security.protocol", "SASL_PLAINTEXT")
                .put("sasl.kerberos.service.name", "kafka");
    }

    public Properties getProperties() {
        return properties;
    }

    //生成jaas.conf临时文件
    public static void configureJAAS(String keyTab, String principal) {
        String content = String.format(JAAS_TEMPLATE, keyTab, principal);

        File jaasConf = null;
        PrintWriter writer = null;

        try {

            jaasConf  = File.createTempFile("jaas", ".conf");
            writer = new PrintWriter(jaasConf);

            writer.println(content);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } finally {

            if (writer != null) {
                writer.close();
            }

            jaasConf.deleteOnExit();
        }

        System.setProperty("java.security.auth.login.config", jaasConf.getAbsolutePath());

    }

}

