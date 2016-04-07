package com.hello.biggudeta.firmware;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ksg on 4/6/16
 */
public class FirmwareAnalyticsDAODynamoDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirmwareAnalyticsDAODynamoDB.class);

    public static DynamoDB getDynamoDBConnection() {

        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxErrorRetry(3);

        final AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain(), clientConfiguration);
        dynamoDBClient.setEndpoint("http://dynamodb.us-east-1.amazonaws.com");

        return new DynamoDB(dynamoDBClient);
    }

    public static void writeToDynamoDB(final FirmwareAnalytics data, final DateTime updated, final DynamoDB dynamoDB, final String tableName) {
        String [] split = data.buckString.split(" \\| ", 2);
        if (split.length != 2) {
            LOGGER.error("action=split-bucket-string, error=not-expected-size size={}, string={}", split.length, data.buckString);
            return;
        }

        final String firmwareVersion = split[1];
        final String timeBucket = split[0];

        final Item ddbItem = new Item()
                .withPrimaryKey("fw_version", firmwareVersion,
                        "datetime", timeBucket)
                .withInt("counts", data.counts)
                .withLong("total_uptime", data.totalUptime)
                .withInt("avg_uptime", data.avgUpTime)
                .withInt("min_uptime", data.minUpTime)
                .withInt("max_uptime", data.maxUptime)
                .withString("batch_start", data.batchStartTime.toString())
                .withString("batch_end", data.batchEndTime.toString())
                .withString("updated", updated.toString());

        dynamoDB.getTable(tableName).putItem(ddbItem);
    }
}