package com.kevin.gps.kafka.config;

public class AppConfig {
    // MAX THREADS ALLOWED
    public static final int MAX_THREADS = 2000;

    // KAFKA
    public final static String SCHEMA_REGISTRY = "schema.registry.url";
    public final static String SCHEMA_URL = "http://127.0.0.1:8081";

    public final static String producerApplicationID = "GPS-Data-Simulator";
    public final static String topicName = "district-VCL";
    public final static String[] eventFiles = {
            "data/218790.csv",
            "data/235678.csv",
            "data/100121.csv",
            "data/100123.csv",
            "data/100134.csv",
            "data/100234.csv",
            "data/100235.csv",
            "data/100345.csv",
            "data/100411.csv",
            "data/100421.csv",
            "data/230011.csv",
            "data/240023.csv",
            "data/230084.csv",
            "data/890023.csv",
            "data/290012.csv",
            "data/270012.csv",
            "data/210092.csv",
            "data/123345.csv",
    };

    public final static String AUTO_COMMIT = "false";
    public final static String OFFSET_RESET = "earliest";

    public final static String AVRO_READER = "specific.avro.reader";
    public final static String SET_AVRO = "true";

    public final static String consumerApplicationID = "kafkaConsumerToSqlDb";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String groupID = "gpsGroupID";

    // SQL
    public static final String USERNAME = "root";
    public static final String PASSWORD = "password123";
    public static final String CONNECTION = "jdbc:mysql://localhost/javaTest";
}
