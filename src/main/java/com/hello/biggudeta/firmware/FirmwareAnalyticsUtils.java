package com.hello.biggudeta.firmware;

import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import scala.Tuple2;

/**
 * Created by ksg on 4/6/16
 */
public class FirmwareAnalyticsUtils {

    public static PairFunction<Tuple2<String, Iterable<FirmwareStreamData>>, String, FirmwareAnalytics> PROCESS_DATA =
            tuple -> {
                final String dateFW = tuple._1();

                int counts = 0;
                long sum = 0;
                int minUptime = Integer.MAX_VALUE;
                int maxUptime = Integer.MIN_VALUE;
                long maxTimestamp = Long.MIN_VALUE;
                long minTimestamp = Long.MAX_VALUE;

                for (FirmwareStreamData streamData : tuple._2()) {
                    sum += streamData.upTime;
                    counts++;
                    minTimestamp = Math.min(minTimestamp, streamData.timestampMillis);
                    maxTimestamp = Math.max(maxTimestamp, streamData.timestampMillis);
                    minUptime = Math.min(minUptime, streamData.upTime);
                    maxUptime = Math.max(maxUptime, streamData.upTime);
                }

                final int average = (int) ((double) sum/counts);
                final DateTime maxDate = new DateTime(maxTimestamp, DateTimeZone.UTC);
                final DateTime minDate = new DateTime(minTimestamp, DateTimeZone.UTC);

                return new Tuple2<>(dateFW, new FirmwareAnalytics(dateFW, minDate, maxDate, counts, sum, minUptime, maxUptime, average));
            };
}
