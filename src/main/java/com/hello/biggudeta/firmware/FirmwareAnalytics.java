package com.hello.biggudeta.firmware;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * Created by ksg on 4/5/16
 */
public class FirmwareAnalytics implements Serializable {
    public final String buckString; // "date | fw_version"
    public final DateTime batchStartTime;
    public final DateTime batchEndTime;
    public final Integer counts; // unique device
    public final Long totalUptime;
    public final Integer minUpTime;
    public final Integer maxUptime;
    public final Integer avgUpTime;

    public FirmwareAnalytics(final String buckString,
                             final DateTime batchStartTime,
                             final DateTime batchEndTime,
                             final Integer counts,
                             final Long totalUptime,
                             final Integer minUpTime,
                             final Integer maxUptime,
                             final Integer avgUpTime) {
        this.buckString = buckString;
        this.batchStartTime = batchStartTime;
        this.batchEndTime = batchEndTime;
        this.counts = counts;
        this.totalUptime = totalUptime;
        this.minUpTime = minUpTime;
        this.maxUptime = maxUptime;
        this.avgUpTime = avgUpTime;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(FirmwareAnalytics.class)
                .add("bucket", buckString)
                .add("counts", counts)
                .add("batch_start", batchStartTime)
                .add("batch_end", batchEndTime)
                .add("total_uptime", totalUptime)
                .add("min_uptime", minUpTime)
                .add("max_uptime", maxUptime)
                .add("average_uptime", avgUpTime)
                .toString();
    }
}
