package com.hello.biggudeta.firmware;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import com.hello.suripu.api.input.DataInputProtos;
import org.apache.spark.SparkConf;
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
import java.util.Map;

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

        final int numStreams = kinesisClient.describeStream(configuration.getKinesisConfiguration().getStreamName()).getStreamDescription().getShards().size();

        final Duration batchInterval = new Duration(configuration.getSparkConfiguration().getBatchIntervalMillis());

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
                            configuration.getKinesisConfiguration().getRegion(),
                            InitialPositionInStream.LATEST,
                            batchInterval, // kinesis checkpoint interval
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
                    final DateTime receivedDT = new DateTime(data.getReceivedAt(), DateTimeZone.UTC);
//                    final int receivedSecs = receivedDT.getSecondOfMinute();
//                    final int setSeconds = (receivedSecs >= 30) ? 30 : 0;
                    final String dateTime = receivedDT.withSecondOfMinute(0).toString("yyyy-MM-dd HH:mm");
                    final String keyString = String.format("%s | %s", dateTime, fwVersion);
                    return new Tuple2<>(keyString,
                            new FirmwareStreamData(fwVersion, data.getUptimeInSecond(), data.getData().getDeviceId(), dateTime, data.getReceivedAt()));
                }
        );

        // Average uptime per FW-version
        JavaPairDStream<String, Iterable<FirmwareStreamData>> uptimeGroupByDateTime = fwUptimeTuple.groupByKey();

        // get unique device-ids, latest protobuf message
        JavaPairDStream<String, Iterable<FirmwareStreamData>> aggregate = uptimeGroupByDateTime.mapValues(values -> {
            final Map<String, FirmwareStreamData> finalSet = Maps.newHashMap();
            int counts = 0;
            for (FirmwareStreamData data : values) {
                final String deviceId = data.deviceId;
                finalSet.put(deviceId, data);
                counts++;
            }
            System.out.println("original: " + counts + " final: " + finalSet.size());
            return finalSet.values();
        });


        // compute min, max and average
        JavaPairDStream<String, FirmwareAnalytics> averageUptime = aggregate.mapToPair(FirmwareAnalyticsUtils.PROCESS_DATA);


        // print something
        averageUptime.print();

        // save data to dynamo
        String dynamoDBTable = configuration.getDynamoDBConfiguration().getTableName();
        averageUptime.foreachRDD(rdd -> {

            rdd.foreachPartition(partitionRecord -> {

                final DynamoDB dynamoDB = FirmwareAnalyticsDAODynamoDB.getDynamoDBConnection();

                partitionRecord.forEachRemaining(record -> {
                    final DateTime now = DateTime.now(DateTimeZone.UTC);
                    FirmwareAnalyticsDAODynamoDB.writeToDynamoDB(record._2(), now, dynamoDB, dynamoDBTable);
                    System.out.println("Now: " + now + "Data: " + record._2().toString());
                });

            });
        });

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}