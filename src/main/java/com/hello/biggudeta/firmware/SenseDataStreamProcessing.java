package com.hello.biggudeta.firmware;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hello.suripu.api.input.DataInputProtos;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ksg on 3/24/16
 */
public class SenseDataStreamProcessing {
    private static final Logger LOGGER = LoggerFactory.getLogger(SenseDataStreamProcessing.class);

    private static DynamoDB getDynamoDBConnection() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxErrorRetry(3);

        final AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain(), clientConfiguration);
        dynamoDBClient.setEndpoint("http://dynamodb.us-east-1.amazonaws.com");

        return new DynamoDB(dynamoDBClient);
    }

    private static void writeToDynamoDB(final FirmwareAnalytics data, final DateTime updated, final DynamoDB dynamoDB, final String tableName) {
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

    public static void main(String[] args) {

        final String configurationFile = args[0];

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        SenseDataStreamConfiguration configuration = new SenseDataStreamConfiguration();
        try {
            configuration = mapper.readValue(new File(configurationFile), SenseDataStreamConfiguration.class);
        } catch (IOException e) {
            LOGGER.error("action=read-configuration error=something-wrong-aborting");
            e.printStackTrace();
            System.exit(1);
        }

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        // setup Kinesis
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(awsCredentialsProvider);
        final String kinesisEndpoint = configuration.getKinesisConfiguration().getEndpoint();
        kinesisClient.setEndpoint(kinesisEndpoint);

        final int numShards = kinesisClient.describeStream(configuration.getKinesisConfiguration().getStreamName()).getStreamDescription().getShards().size();
        final int numStreams = numShards;

        final Duration batchInterval = new Duration(configuration.getSparkConfiguration().getBatchIntervalMillis());

        final Duration kinesisCheckpointInterval = batchInterval;


        // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
        // DynamoDB of the same region as Kinesis stream
        final String regionName = RegionUtils.getRegionByEndpoint(kinesisEndpoint).getName();


        // set up Spark Context
        SparkConf sparkConfig = new SparkConf()
                .setMaster(configuration.getSparkConfiguration().getMasterConfig())
                .setAppName(configuration.getSparkConfiguration().getAppName());

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);
        jssc.checkpoint(configuration.getSparkConfiguration().getCheckpointDirectory());

        // Create Kinesis DStreams
        final List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            streamsList.add(
                    KinesisUtils.createStream(jssc,
                            configuration.getKinesisConfiguration().getAppName(),
                            configuration.getKinesisConfiguration().getStreamName(),
                            kinesisEndpoint,
                            regionName,
                            InitialPositionInStream.LATEST,
                            kinesisCheckpointInterval,
                            StorageLevel.MEMORY_ONLY())
            );
        }

        // start processing

        // union the streams
        final JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            unionStreams = streamsList.get(0);
        }

        // convert each stream to DataInputProtos.BatchPeriodicDataWorker
        JavaDStream<DataInputProtos.BatchPeriodicDataWorker> batchData = unionStreams.flatMap(
                bytes -> Arrays.asList(DataInputProtos.BatchPeriodicDataWorker.parseFrom(bytes)));

        // Create batches of <FW-version, uptime>
        JavaPairDStream<String, FirmwareStreamData> fwUptimeTuple =  batchData.mapToPair(
                data -> {
                    final String fwVersion = String.format("%s_%s", data.getFirmwareMiddleVersion(), data.getFirmwareTopVersion());
                    return new Tuple2<>(fwVersion,
                            new FirmwareStreamData(fwVersion, data.getUptimeInSecond(), data.getReceivedAt()));
                }
        );

        // Average uptimes per FW-version
        JavaPairDStream<String, Iterable<FirmwareStreamData>> uptimeGroupByFW = fwUptimeTuple.groupByKey();

        JavaPairDStream<String, FirmwareAnalytics> averageUptime = uptimeGroupByFW.mapToPair(
                new PairFunction<Tuple2<String, Iterable<FirmwareStreamData>>, String, FirmwareAnalytics>() {
                    @Override
                    public Tuple2<String, FirmwareAnalytics> call(Tuple2<String, Iterable<FirmwareStreamData>> tuple) throws Exception {
                        final String fwVersion = tuple._1();
                        int counts = 0;
                        int sum = 0;
                        int minUptime = Integer.MAX_VALUE;
                        int maxUptime = Integer.MIN_VALUE;
                        long maxTimestamp = Long.MIN_VALUE;
                        for (FirmwareStreamData streamData : tuple._2()) {
                            sum += streamData.upTime;
                            counts++;
                            if (streamData.upTime < minUptime) { minUptime = streamData.upTime; }
                            if (streamData.upTime > maxUptime) { maxUptime = streamData.upTime; }
                            if (streamData.timestampMillis > maxTimestamp) { maxTimestamp = streamData.timestampMillis; }
                        }
                        final int average = (int) ((float) sum/counts);
                        final DateTime date = new DateTime(maxTimestamp, DateTimeZone.UTC);
                        return new Tuple2<>(fwVersion, new FirmwareAnalytics(fwVersion, date, counts, minUptime, maxUptime, average));
                    }
                }
        );

//        uptimeGroupByFW.print();
//        averageUptimeByFW.print();

        // save data to dynamo
        final String dynamoDBTable = configuration.getDynamoDBConfiguration().getTableName();
        averageUptime.foreachRDD(rdd -> {

            rdd.foreachPartition(partitionRecord -> {

                DynamoDB dynamoDB = getDynamoDBConnection();

                partitionRecord.forEachRemaining(record -> {
                    final DateTime now = DateTime.now(DateTimeZone.UTC);
                    writeToDynamoDB(record._2(), now, dynamoDB, dynamoDBTable);
                    System.out.println("Now: " + now + "Data: " + record._2().toString());
                });

            });
        });

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}