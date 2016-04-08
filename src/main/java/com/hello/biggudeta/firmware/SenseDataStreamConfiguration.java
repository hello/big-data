package com.hello.biggudeta.firmware;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by ksg on 4/6/16
 */
public class SenseDataStreamConfiguration {

    class SparkConfiguration {

        @JsonProperty("streaming_app_name")
        private String appName;
        public String getAppName() { return appName; }

        @JsonProperty("master")
        private String masterConfig;
        public String getMasterConfig() {return masterConfig; }

        @JsonProperty("batch_interval_millis")
        private long batchIntervalMillis;
        public long getBatchIntervalMillis() { return batchIntervalMillis; }

        @JsonProperty("checkpoint_directory")
        private String checkpointDirectory;
        public String getCheckpointDirectory() { return  checkpointDirectory; }

        @JsonProperty("window_size_millis")
        private long windowSizeMillis;
        public long getWindowSizeMillis() { return windowSizeMillis; }

        public SparkConfiguration() {}
    }

    class KinesisConfiguration {
        @JsonProperty("stream_name")
        private String streamName;
        public String getStreamName() { return streamName; }

        @JsonProperty("endpoint")
        private String endpoint;
        public String getEndpoint() { return endpoint; }

        @JsonProperty("app_name")
        private String appName;
        public String getAppName() { return appName; }

        @JsonProperty("region")
        private String region;
        public String getRegion() { return region; }


        public KinesisConfiguration() {}
    }

    public class DynamoDBConfiguration {

        @JsonProperty("region")
        private String region;
        public String getRegion() { return region; }

        @JsonProperty("endpoint")
        private String endpoint;
        public String getEndpoint() { return endpoint; }

        @JsonProperty("table_name")
        private String tableName;
        public String getTableName() { return tableName; }

        public DynamoDBConfiguration() {}
    }

    @JsonProperty("spark")
    private SparkConfiguration sparkConfiguration;
    public SparkConfiguration getSparkConfiguration(){
        return sparkConfiguration;
    }

    @JsonProperty("kinesis")
    private KinesisConfiguration kinesisConfiguration;
    public KinesisConfiguration getKinesisConfiguration(){
        return kinesisConfiguration;
    }

    @JsonProperty("dynamodb")
    private DynamoDBConfiguration dynamoDBConfiguration;
    public DynamoDBConfiguration getDynamoDBConfiguration(){
        return dynamoDBConfiguration;
    }


}