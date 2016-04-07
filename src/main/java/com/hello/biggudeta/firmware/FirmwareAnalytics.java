package com.hello.biggudeta.firmware;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

/**
 * Created by ksg on 4/5/16
 */
public class FirmwareAnalytics {
    public final String firmwareVersion;
    public final DateTime dateTime;
    public final Integer counts;
    public final Integer minUpTime;
    public final Integer maxUptime;
    public final Integer avgUpTime;

    public FirmwareAnalytics(final String firmwareVersion, final DateTime dateTime, final Integer counts,
                             final Integer minUpTime, final Integer maxUptime, final Integer avgUpTime) {
        this.firmwareVersion = firmwareVersion;
        this.dateTime = dateTime;
        this.counts = counts;
        this.minUpTime = minUpTime;
        this.maxUptime = maxUptime;
        this.avgUpTime = avgUpTime;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(FirmwareAnalytics.class)
                .add("firmware_version", firmwareVersion)
                .add("counts", counts)
                .add("datetime", dateTime)
                .add("min_uptime", minUpTime)
                .add("max_uptime", maxUptime)
                .add("average_uptime", avgUpTime)
                .toString();
    }
}
