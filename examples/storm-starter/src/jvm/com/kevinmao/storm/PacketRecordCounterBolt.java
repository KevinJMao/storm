package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kevinmao.graphite.GraphiteCodec;
import com.kevinmao.util.Packet;
import org.apache.log4j.Logger;

import java.util.Map;

public class PacketRecordCounterBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(PacketRecordCounterBolt.class);
    private double countingTimeWindow;
    private OutputCollector collector;
    private int timeIndex;
    private long packetCount;
    private double nextEmit;
    private long lastRecordTimestampSeconds;

    public PacketRecordCounterBolt(double countingTimeWindow) {
        this.countingTimeWindow = countingTimeWindow;
        this.timeIndex = 1;
        packetCount = 0;
        nextEmit = countingTimeWindow;
        lastRecordTimestampSeconds = AttackDetectionTopology.TOPOLOGY_START_TIME_MILLIS;
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Packet record = (Packet) input.getValueByField(AttackDetectionTopology.DECODER_BOLT_PACKET_RECORD_OUTPUT_FIELD);
        collector.ack(input);
        if(record.getTimestamp() >= nextEmit) {
            LOG.info("Emitting value:" + new Values(timeIndex, packetCount, lastRecordTimestampSeconds));
            collector.emit(new Values(timeIndex, packetCount, lastRecordTimestampSeconds));
            timeIndex++;
            nextEmit += countingTimeWindow;
            packetCount = 0;
        } else {
            packetCount++;
            Long timestampOffsetMillis = (long) (record.getTimestamp() * 1000.0);
            lastRecordTimestampSeconds = (AttackDetectionTopology.TOPOLOGY_START_TIME_MILLIS + timestampOffsetMillis) / 1000L;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.COUNTER_BOLT_TIME_INDEX_FIELD,
                AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD,
                AttackDetectionTopology.LAST_TIMESTAMP_MEASURED));
    }
}

class PacketRecordCounterGraphiteWriterBolt extends GraphiteWriterBoltBase {
    public PacketRecordCounterGraphiteWriterBolt(String graphiteServerHostname, int graphiteServerPortNumber) {
        super(graphiteServerHostname, graphiteServerPortNumber);
    }
    @Override
    public void execute(Tuple input) {
        Long actualPacketCount = Long.parseLong(input.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD).toString());
        Long timestamp = Long.parseLong(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".actualPacketCount", GraphiteCodec.format(actualPacketCount) , timestamp);
        super.collector.ack(input);
    }
}