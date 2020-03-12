package com.kevin.gps.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class RunnableProducer implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private final AtomicBoolean stopper = new AtomicBoolean(false);
    private String fileLocation;
    private String topicName;
    private KafkaProducer<String, Location> producer;

    RunnableProducer(KafkaProducer<String, Location> producer, String topicName, String fileLocation) {
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = fileLocation;
    }

    private String getDistrictID() {
        return "142847";
    }

    private String getVehicleID(String fileLocation) {
        String[] str = fileLocation.split("[/.]");
        return str[1];
    }

    @Override
    public void run() {
        logger.info("Start Processing " + fileLocation);
        File file = new File(fileLocation);

        try(Scanner scanner = new Scanner(file)) {
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] arr = line.split(",");

                String vehicleID = getVehicleID(fileLocation);
                String districtID = getDistrictID();

                Location location = Location.newBuilder()
                        .setDistrictID(districtID)
                        .setVehicleID(vehicleID)
                        .setLat(Double.parseDouble(arr[0]))
                        .setLng(Double.parseDouble(arr[1]))
                        .build();

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    logger.info("Thread Interrupted");
                }

                System.out.println(location);
                producer.send(new ProducerRecord<String, Location>(topicName, vehicleID, location));
            }
        } catch(FileNotFoundException e) {
            logger.info("File Not Found - " + e);
        }
    }

    void shutdown() {
        logger.info("Shutting down producer thread...");
        stopper.set(true);
    }
}
