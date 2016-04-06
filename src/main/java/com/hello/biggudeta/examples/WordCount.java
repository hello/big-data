package com.hello.biggudeta.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by kingshy on 3/23/16
 */
public class WordCount
{
    private static Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
            new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            };

    private static final PairFunction<String, String, Integer> WORDS_MAPPER =
            new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s, 1);
                }
            };

    private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
            new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer a, Integer b) throws Exception {
                    return a + b;
                }
            };

    private static final Function<String, Integer> LINE_SIZER =
            new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return s.length();
                }
            };

    static class GetLineLength implements Function<String, Integer> {
        @Override
        public Integer call(String s) throws Exception {
            return s.length();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("input_file output_dir");
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("com.example.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        // read file
        JavaRDD<String> file = context.textFile(args[0]);

        // Word Count
        JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
        JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER).sortByKey(); // make similar keys be close
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

        // Ways to print
        final Map<String, Integer> wordCounterMap = counter.collectAsMap();
        for (Map.Entry<String, Integer> entry: wordCounterMap.entrySet()) {
            if (entry.getValue() > 10) {
                LOGGER.info("{} = {}", entry.getKey(), entry.getValue());
            }
        }

        for (Tuple2<String, Integer> tuple : counter.sortByKey().collect()) {
            if (tuple._2() > 10) {
                System.out.println(String.format("%s, %s = %d", tuple, tuple._1(), tuple._2()));
            }
        }

        // write data to file (tuples)
//        counter.saveAsTextFile(args[1]);

        JavaRDD<Integer> lengths = file.map(LINE_SIZER);
        JavaRDD<Integer> lengths2 = file.map(new GetLineLength());
        final Integer totalLength = lengths.reduce(WORDS_REDUCER);
        final Integer totalLength2 = lengths2.reduce(WORDS_REDUCER);
        LOGGER.info("Length of file {}: {}, {}", args[0], totalLength, totalLength2);
        LOGGER.info("ALL DONE!!!!!!!!!");

    }
}
