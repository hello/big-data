package com.hello.biggudeta.firmware;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
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

    private static PairFunction<Tuple2<String, Iterable<FirmwareStreamData>>, String, FirmwareAnalytics> PROCESS_DATA =
            tuple -> {
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
            };


    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        SenseDataStreamConfiguration configuration = new SenseDataStreamConfiguration();
        try {
            configuration = mapper.readValue(new File(args[0]), SenseDataStreamConfiguration.class);
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
                    return new Tuple2<>(fwVersion,
                            new FirmwareStreamData(fwVersion, data.getUptimeInSecond(), data.getReceivedAt()));
                }
        );

        // Average uptime per FW-version
        JavaPairDStream<String, Iterable<FirmwareStreamData>> uptimeGroupByFW = fwUptimeTuple.groupByKey();

        // compute min, max and average
        JavaPairDStream<String, FirmwareAnalytics> averageUptime = uptimeGroupByFW.mapToPair(PROCESS_DATA);

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