package com.hello.biggudeta.firmware;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Created by ksg on 4/5/16
 */
public class FirmwareStreamData implements Serializable{
    public final String firmwareVersion;
    public final Integer upTime;
    public final String deviceId;
    public final String dateTime;
    public final Long timestampMillis;

    public FirmwareStreamData(final String firmwareVersion, final Integer upTime,
                              final String deviceId, final String dateTime, final Long timestampMillis) {
        this.firmwareVersion = firmwareVersion;
        this.upTime = upTime;
        this.deviceId = deviceId;
        this.dateTime = dateTime;
        this.timestampMillis = timestampMillis;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(FirmwareStreamData.class)
                .add("firmware_version", firmwareVersion)
                .add("uptime", upTime)
                .add("timestamp", timestampMillis)
                .toString();
    }

}
