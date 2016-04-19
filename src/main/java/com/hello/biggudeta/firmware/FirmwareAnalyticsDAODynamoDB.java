package com.hello.biggudeta.firmware;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ksg on 4/6/16
 */
public class FirmwareAnalyticsDAODynamoDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirmwareAnalyticsDAODynamoDB.class);

    // DynamoDB table attributes
    private static final String ATTRIBUTE_FIRMWARE_VERSION = "fw";
    private static final String ATTRIBUTE_DATE_BUCKET = "date";
    private static final String ATTRIBUTE_COUNTS = "c";
    private static final String ATTRIBUTE_TOTAL_UPTIME = "up_tot";
    private static final String ATTRIBUTE_AVERAGE_UPTIME = "up_avg";
    private static final String ATTRIBUTE_MIN_UPTIME = "up_min";
    private static final String ATTRIBUTE_MAX_UPTIME = "up_max";
    private static final String ATTRIBUTE_BATCH_START_TIME = "b_start";
    private static final String ATTRIBUTE_BATCH_END_TIME = "b_end";
    private static final String ATTRIBUTE_UPDATED = "updated";

    public static DynamoDB getDynamoDBConnection(final String endpoint) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxErrorRetry(3);

        final AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain(), clientConfiguration);
        dynamoDBClient.setEndpoint(endpoint);

        return new DynamoDB(dynamoDBClient);
    }


    public static void writeToDynamoDB(final FirmwareAnalytics data, final DateTime updated, final Table dynamoDBTable) {

        // split string to get FW and time bucket
        String[] split = data.buckString.split(" \\| ", 2);
        if (split.length != 2) {
            LOGGER.error("action=split-bucket-string, error=not-expected-size size={}, string={}", split.length, data.buckString);
            return;
        }

        final String firmwareVersion = split[1];
        final String timeBucket = split[0];


        final FirmwareAnalytics existingData = getItem(dynamoDBTable, firmwareVersion, timeBucket);

        if (existingData == null) {

            // nothing exist for this FW-time-bucket
            putItem(dynamoDBTable, firmwareVersion, timeBucket, data, updated);

        } else {

            final int total = data.counts + existingData.counts;
            final long newSum = data.totalUptime + existingData.totalUptime;
            final int avg = (int) ((double) newSum / total);
            final FirmwareAnalytics newData = new FirmwareAnalytics(
                    "whatever",
                    existingData.batchStartTime.isBefore(data.batchStartTime) ? existingData.batchStartTime : data.batchStartTime,
                    data.batchEndTime.isAfter(data.batchEndTime) ? data.batchEndTime : existingData.batchEndTime,
                    total,
                    newSum,
                    Math.min(existingData.minUpTime, data.minUpTime),
                    Math.min(existingData.maxUptime, data.maxUptime),
                    avg);

            putItem(dynamoDBTable, firmwareVersion, timeBucket, newData, updated);
        }
    }


    protected static FirmwareAnalytics getItem(final Table dynamoTable, final String hashKey, final String rangeKey) {

        final Item item = dynamoTable.getItem(ATTRIBUTE_FIRMWARE_VERSION, hashKey, ATTRIBUTE_DATE_BUCKET, rangeKey);

        if (item == null) {
            return null;
        }

        return new FirmwareAnalytics("existing",
                DateTime.parse(item.get(ATTRIBUTE_BATCH_START_TIME).toString(), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")),
                DateTime.parse(item.get(ATTRIBUTE_BATCH_END_TIME).toString(), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")),
                Integer.valueOf(item.get(ATTRIBUTE_COUNTS).toString()),
                Long.valueOf(item.get(ATTRIBUTE_TOTAL_UPTIME).toString()),
                Integer.valueOf(item.get(ATTRIBUTE_MIN_UPTIME).toString()),
                Integer.valueOf(item.get(ATTRIBUTE_MAX_UPTIME).toString()),
                Integer.valueOf(item.get(ATTRIBUTE_AVERAGE_UPTIME).toString()));
    }


    protected static void putItem(final Table dynamoTable, final String hashKey, final String rangeKey, final FirmwareAnalytics data, final DateTime updated) {

        final Item ddbItem = new Item()
                .withPrimaryKey(ATTRIBUTE_FIRMWARE_VERSION, hashKey, ATTRIBUTE_DATE_BUCKET, rangeKey)
                .withInt(ATTRIBUTE_COUNTS, data.counts)
                .withLong(ATTRIBUTE_TOTAL_UPTIME, data.totalUptime)
                .withInt(ATTRIBUTE_AVERAGE_UPTIME, data.avgUpTime)
                .withInt(ATTRIBUTE_MIN_UPTIME, data.minUpTime)
                .withInt(ATTRIBUTE_MAX_UPTIME, data.maxUptime)
                .withString(ATTRIBUTE_BATCH_START_TIME, data.batchStartTime.toString("yyyy-MM-dd HH:mm:ss"))
                .withString(ATTRIBUTE_BATCH_END_TIME, data.batchEndTime.toString("yyyy-MM-dd HH:mm:ss"))
                .withString(ATTRIBUTE_UPDATED, updated.toString());

        dynamoTable.putItem(ddbItem);
    }
}