package com.kevin.gps.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RunnableConsumer implements Runnable {
    private static final Logger logger = LogManager.getLogger();

    private ConsumerRecord<String, Location> record;
    private Connection connection;

    RunnableConsumer(ConsumerRecord<String, Location> record, Connection connection) {
        this.record = record;
        this.connection = connection;
    }

    @Override
    public void run() {
        // Send value to DB
        try {
            String query = " insert into testTable (districtID, vehicleID, lat, lng)" + " values (?,?,?,?)";
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, record.value().getDistrictID());
            stmt.setString(2, record.value().getVehicleID());
            stmt.setDouble(3, record.value().getLat());
            stmt.setDouble(4, record.value().getLng());
            stmt.execute();
        } catch(SQLException e) {
            logger.info("%SQL-ERROR%: SQL execution failed! Message - " + e);
        }
    }
}
