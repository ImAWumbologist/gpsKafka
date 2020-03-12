package com.kevin.gps.kafka;

import com.kevin.gps.kafka.config.AppConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GpsConsumer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties consumerProps = new Properties();

        // Standard properties
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, AppConfig.consumerApplicationID);
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.groupID);
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AppConfig.AUTO_COMMIT);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConfig.OFFSET_RESET);

        // AVRO
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.setProperty(AppConfig.SCHEMA_REGISTRY, AppConfig.SCHEMA_URL);
        consumerProps.setProperty(AppConfig.AVRO_READER, AppConfig.SET_AVRO);

        // Create Consumer and subscribe to topic
        KafkaConsumer<String, Location> consumer = new KafkaConsumer<String, Location>(consumerProps);
        consumer.subscribe(Collections.singleton(AppConfig.topicName));

        // SQL Connector
        Connection sqlConnection = null;

        // Connect to DB
        try {
            sqlConnection = DriverManager.getConnection(AppConfig.CONNECTION, AppConfig.USERNAME, AppConfig.PASSWORD);
            logger.info("Connected to MySQL...");
        } catch (SQLException e) {
            logger.info("%SQL-ERROR%: SQL execution failed! Message - " + e);
        }

        // Start Consumer
        if(sqlConnection != null) {
            logger.info("Consumer Started...");

            // Create ExecutorService to manage our maximum allocated number of threads
            ExecutorService executor = Executors.newFixedThreadPool(AppConfig.MAX_THREADS);

            while(true) {
                // Get a poll of records every second
                ConsumerRecords<String, Location> records = consumer.poll(Duration.ofSeconds(1));

                for(ConsumerRecord<String, Location> record : records) {
                    RunnableConsumer runnableConsumer = new RunnableConsumer(record, sqlConnection);
                    executor.execute(runnableConsumer);
                }
            }
        }
    }
}
