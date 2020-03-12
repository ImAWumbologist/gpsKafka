package com.kevin.gps.kafka;

import com.kevin.gps.kafka.config.AppConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GpsProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties producerProps = new Properties();

        // Producer Properties
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.producerApplicationID);
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerProps.setProperty(AppConfig.SCHEMA_REGISTRY, AppConfig.SCHEMA_URL);

        // Create a Producer and Executor to manage threads
        KafkaProducer<String, Location> producer = new KafkaProducer<String, Location>(producerProps);
        ExecutorService executor = Executors.newFixedThreadPool(AppConfig.MAX_THREADS);
        final List<RunnableProducer> runnableProducers = new ArrayList<>();

        // Make a thread for each file
        for(int i = 0; i < AppConfig.eventFiles.length; i++) {
            RunnableProducer runnableProducer = new RunnableProducer(producer, AppConfig.topicName, AppConfig.eventFiles[i]);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        // On shutdown...
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RunnableProducer p : runnableProducers) p.shutdown();
            executor.shutdown();
            logger.info("Closing Executor Service");
            try {
                executor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

    }

}
