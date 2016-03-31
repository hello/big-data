package com.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.hello.suripu.api.input.DataInputProtos;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ksg on 3/24/16
 */
public class SenseDataStreamProcessing {
    private static final Logger LOGGER = LoggerFactory.getLogger(SenseDataStreamProcessing.class);

    private static Long SPARK_STREAMING_BATCH_INTERVAL_MILLIS = 20 * 1000L; // 5 minutes
    private static String KINESIS_STREAM_NAME = "dev_sense_sensors_data";
    private static String KINESIS_APP_NAME = "SenseSaveConsumerDevDDBKSG_2";
    private static String KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";
    private static String STREAMING_APP_NAME = "SenseDataKinesisProcessing";


    public static void main(String[] args) {

//        String workerId;
//        try {
//            workerId = InetAddress.getLocalHost().getCanonicalHostName();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//            workerId = "localhost";
//        }

        final AmazonKinesisClient kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        kinesisClient.setEndpoint(KINESIS_ENDPOINT);

        final int numShards = kinesisClient.describeStream(KINESIS_STREAM_NAME).getStreamDescription().getShards().size();
        final int numStreams = numShards;

        final Duration batchInterval =  Durations.seconds(10); // new Duration(SPARK_STREAMING_BATCH_INTERVAL_MILLIS);

        final Duration kinesisCheckpointInterval = batchInterval;


        // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
        // DynamoDB of the same region as Kinesis stream
        final String regionName = RegionUtils.getRegionByEndpoint(KINESIS_ENDPOINT).getName();


        // set up Spark Context
        final SparkConf sparkConfig = new SparkConf().setMaster("local[4]").setAppName(STREAMING_APP_NAME);
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        // Create Kinesis DStreams
//        final List<JavaDStream<byte[]>> streamsList = new ArrayList<JavaDStream<byte[]>>(numStreams);
//        for (int i = 0; i < numStreams; i++) {
//            streamsList.add(
//                    KinesisUtils.createStream(jssc, KINESIS_APP_NAME, KINESIS_STREAM_NAME, KINESIS_ENDPOINT, regionName,
//                            InitialPositionInStream.LATEST,
//                            kinesisCheckpointInterval,
//                            StorageLevel.MEMORY_AND_DISK_2())
//            );
//        }
//
//        // union the streams
//        final JavaDStream<byte[]> unionStreams;
//        if (streamsList.size() > 1) {
//            unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
//        } else {
//            unionStreams = streamsList.get(0);
//        }

        JavaDStream<byte[]> unionStreams =  KinesisUtils.createStream(
                jssc, KINESIS_APP_NAME, KINESIS_STREAM_NAME, KINESIS_ENDPOINT, regionName,
                InitialPositionInStream.LATEST,
                kinesisCheckpointInterval,
                StorageLevel.MEMORY_AND_DISK_2());

        // convert each stream to DataInputProtos.BatchPeriodicDataWorker
        final JavaDStream<String> batchData = unionStreams.flatMap(
                new FlatMapFunction<byte[], String>() {
                    @Override
                    public Iterable<String> call(byte[] bytes) throws Exception {
                        final DataInputProtos.BatchPeriodicDataWorker data = DataInputProtos.BatchPeriodicDataWorker.parseFrom(bytes);
                        LOGGER.debug("grab device {} ts {}", data.getData().getDeviceId(), data.getReceivedAt());
                        return Arrays.asList(data.getData().getDeviceId());
                    }
                }
        );

        // count FWs
        JavaPairDStream<String, Integer> fwCounts = batchData.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String string) throws Exception {
                        return new Tuple2<String, Integer>(string, 1);
                    }
                }

        );
        fwCounts.print();
//
//        // sum uptime per FW version
//        JavaPairDStream<String, Integer> fwUptimeSum =  batchData.mapToPair(
//                new PairFunction<DataInputProtos.BatchPeriodicDataWorker, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(DataInputProtos.BatchPeriodicDataWorker batchPeriodicDataWorker) throws Exception {
//                        return new Tuple2<String, Integer>(batchPeriodicDataWorker.getFirmwareMiddleVersion(), batchPeriodicDataWorker.getUptimeInSecond());
//                    }
//                }
//        ).reduceByKey(
//                new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer integer, Integer integer2) throws Exception {
//                        return (integer + integer2);
//                    }
//                }
//        );
//
//        // print first 10
//        fwCounts.print();
//        fwUptimeSum.print();

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}