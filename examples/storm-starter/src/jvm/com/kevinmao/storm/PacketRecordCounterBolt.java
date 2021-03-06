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
    private Double countingTimeWindow;
    private OutputCollector collector;
    private Integer timeIndex;
    private Long packetCount;
    private Double nextEmit;
    private Long lastRecordTimestampSeconds;

    public PacketRecordCounterBolt(double countingTimeWindow) {
        this.countingTimeWindow = countingTimeWindow;
        this.timeIndex = 1;
        packetCount = 0L;
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
//            LOG.debug("Emitting values: (timeIndex : " + timeIndex + "),(packetCount : " + packetCount + "),(lastRecordTimestampSeconds : " + lastRecordTimestampSeconds + ")");
            collector.emit(new Values(timeIndex, packetCount.doubleValue(), lastRecordTimestampSeconds));
            timeIndex++;
            nextEmit += countingTimeWindow;
            packetCount = 0L;
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
    private static final Logger LOG = Logger.getLogger(PacketRecordCounterGraphiteWriterBolt.class);

    public PacketRecordCounterGraphiteWriterBolt(String graphiteServerHostname, int graphiteServerPortNumber) {
        super(graphiteServerHostname, graphiteServerPortNumber);
    }
    @Override
    public void execute(Tuple input) {
        Double actualPacketCount = Double.parseDouble(input.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD).toString());
        Long timestamp = Long.parseLong(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
        LOG.debug("Sending to graphite: (actualPacketCount, " + actualPacketCount + ", " + timestamp + ")");
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".actualPacketCount", GraphiteCodec.format(actualPacketCount) , timestamp);
        super.collector.ack(input);
    }
}