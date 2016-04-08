package com.hello.biggudeta.firmware;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hello.suripu.api.input.DataInputProtos;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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


    public static void main(String[] args) {

        // Read configuration file
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        SenseDataStreamConfiguration configuration = new SenseDataStreamConfiguration();

        try {
            configuration = mapper.readValue(new File(args[0]), SenseDataStreamConfiguration.class);
        } catch (IOException e) {
            LOGGER.error("action=read-configuration error=something-wrong-aborting msg={}", e.getMessage());
            System.exit(1);
        }

        // setup Kinesis
        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        final String kinesisEndpoint = configuration.getKinesisConfiguration().getEndpoint();
        kinesisClient.setEndpoint(kinesisEndpoint);

        final String kinesisStreamName = configuration.getKinesisConfiguration().getStreamName();
        final int numStreams = kinesisClient.describeStream(kinesisStreamName).getStreamDescription().getShards().size();

        // set up Spark Context
        SparkConf sparkConfig = new SparkConf()
                .setMaster(configuration.getSparkConfiguration().getMasterConfig())
                .setAppName(configuration.getSparkConfiguration().getAppName());

        // Receiver batch Interval
        final Duration batchInterval = new Duration(configuration.getSparkConfiguration().getBatchIntervalMillis());

        // accumulate a 1 minute non-overlapping window of data before processing
        final Duration windowSize = new Duration(configuration.getSparkConfiguration().getWindowSizeMillis());

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
                            configuration.getKinesisConfiguration().getRegion(),
                            InitialPositionInStream.LATEST,
                            batchInterval, // kinesis checkpoint interval
                            StorageLevel.MEMORY_ONLY())
            );
        }

        // start processing

        // union the streams from shards
        final JavaDStream<byte[]> unionStreams;
        if (streamsList.size() > 1) {
            unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            unionStreams = streamsList.get(0);
        }


        // convert each stream to DataInputProtos.BatchPeriodicDataWorker
        JavaDStream<DataInputProtos.BatchPeriodicDataWorker> batchData = unionStreams.flatMap(
                bytes -> Arrays.asList(DataInputProtos.BatchPeriodicDataWorker.parseFrom(bytes)));


        // accumulate larger window of data
        JavaDStream<DataInputProtos.BatchPeriodicDataWorker> windowData = batchData.window(windowSize, windowSize);


        // look into each DStream RDD and only save unique data
        JavaDStream<DataInputProtos.BatchPeriodicDataWorker> distinctData = windowData.transform(
                (Function<JavaRDD<DataInputProtos.BatchPeriodicDataWorker>, JavaRDD<DataInputProtos.BatchPeriodicDataWorker>>) JavaRDD::distinct);


        // Create batches of <"date | middle_top-version", FirmwareStreamData>
        JavaPairDStream<String, FirmwareStreamData> fwUptimeTuple = distinctData.mapToPair(
                data -> {
                    final String fwVersion = String.format("%s_%s", data.getFirmwareMiddleVersion(), data.getFirmwareTopVersion());
                    final DateTime receivedDT = new DateTime(data.getReceivedAt(), DateTimeZone.UTC);
                    final String dateTime = receivedDT.withSecondOfMinute(0).toString("yyyy-MM-dd HH:mm");
                    final String keyString = String.format("%s | %s", dateTime, fwVersion);

                    // debugging
                    if (fwVersion.equalsIgnoreCase("FB7_1.0.3")) {
                        System.out.println("bucket: " + keyString + "  recvd: " + receivedDT.toString() +
                                "  uptime: " + data.getUptimeInSecond() + "  device: " + data.getData().getDeviceId());
                    }

                    return new Tuple2<>(keyString,
                            new FirmwareStreamData(fwVersion, data.getUptimeInSecond(), data.getData().getDeviceId(), dateTime, data.getReceivedAt()));
                }
        );


        // Average uptime per FW-version
        JavaPairDStream<String, Iterable<FirmwareStreamData>> uptimeGroupByDateTime = fwUptimeTuple.groupByKey();


        // compute min, max and average
        JavaPairDStream<String, FirmwareAnalytics> averageUptime = uptimeGroupByDateTime.mapToPair(FirmwareAnalyticsUtils.PROCESS_DATA);


        // print something
        averageUptime.print();


        // save data to dynamo
        final String dynamoDBTableName = configuration.getDynamoDBConfiguration().getTableName();
        final String dynamoEndpoint = configuration.getDynamoDBConfiguration().getEndpoint();
        averageUptime.foreachRDD(rdd -> {

            rdd.foreachPartition(partitionRecord -> {

                // TODO: create connection pool
                final DynamoDB dynamoDB = FirmwareAnalyticsDAODynamoDB.getDynamoDBConnection(dynamoEndpoint);
                final Table dynamoDBTable = dynamoDB.getTable(dynamoDBTableName);

                partitionRecord.forEachRemaining(record -> {
                    final DateTime now = DateTime.now(DateTimeZone.UTC);
                    final FirmwareAnalytics analytics = record._2();

                    FirmwareAnalyticsDAODynamoDB.writeToDynamoDB(analytics, now, dynamoDBTable);

//                    // debugging. current main version is F34_1.0.3
//                    if (analytics.buckString.contains("FB7_1.0.3")) {
//                        System.out.println("Now: " + now + "Data: " + analytics.toString());
//                    }
                });

            });
        });

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}