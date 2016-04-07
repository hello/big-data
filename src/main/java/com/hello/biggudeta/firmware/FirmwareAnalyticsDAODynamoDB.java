package com.hello.biggudeta.firmware;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import org.joda.time.DateTime;

/**
 * Created by ksg on 4/6/16
 */
public class FirmwareAnalyticsDAODynamoDB {

    public static DynamoDB getDynamoDBConnection() {

        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxErrorRetry(3);

        final AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain(), clientConfiguration);
        dynamoDBClient.setEndpoint("http://dynamodb.us-east-1.amazonaws.com");

        return new DynamoDB(dynamoDBClient);
    }

    public static void writeToDynamoDB(final FirmwareAnalytics data, final DateTime updated, final DynamoDB dynamoDB, final String tableName) {
        final Item ddbItem = new Item()
                .withPrimaryKey("fw_version", data.firmwareVersion,
                        "datetime", data.dateTime.toString("yyyy-MM-dd HH:mm:ss"))
                .withInt("counts", data.counts)
                .withInt("avg_uptime", data.avgUpTime)
                .withInt("min_uptime", data.minUpTime)
                .withInt("max_uptime", data.maxUptime)
                .withString("updated", updated.toString());

        dynamoDB.getTable(tableName).putItem(ddbItem);
    }
}