package com.kevinmao.util;


import java.io.Serializable;

public class Packet implements Serializable {
    private final long index;
    private final double timestamp;
    private final String source_ip;
    private final String destination_ip;
    private final String protocol;
    private final int length;
    private final String details;

    public Packet(long index, double timestamp, String source_ip, String destination_ip, String protocol, int length, String details) {
        this.index = index;
        this.timestamp = timestamp;
        this.source_ip = source_ip;
        this.destination_ip = destination_ip;
        this.protocol = protocol;
        this.length = length;
        this.details = details;
    }

    public long getIndex() {
        return index;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public String getSource_ip() {
        return source_ip;
    }

    public String getDestination_ip() {
        return destination_ip;
    }

    public String getProtocol() {
        return protocol;
    }

    public int getLength() {
        return length;
    }

    public String getDetails() {
        return details;
    }
}
