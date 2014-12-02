package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kevinmao.graphite.GraphiteCodec;
import org.apache.log4j.Logger;

import java.util.Map;

public class AttackDetectorBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(AttackDetectorBolt.class);
    private OutputCollector collector;

    private double detectionThreshold;

    public AttackDetectorBolt(double detectionThreshold) {
        this.detectionThreshold = detectionThreshold;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        long timestamp = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
        collector.ack(tuple);
        if(Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.CUSUM_MODEL_SUM_OUTPUT_FIELD).toString()) > detectionThreshold) {
            LOG.info("Emitting value: " + new Values(true, timestamp));
            collector.emit(new Values(true, timestamp));
        } else {
            LOG.info("Emitting value:" + new Values(true, timestamp));
            collector.emit(new Values(false, timestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.ATTACK_DETECTOR_DETECTION_OUTPUT_FIELD,
                AttackDetectionTopology.LAST_TIMESTAMP_MEASURED));
    }
}

class AttackDetectorGraphiteWriterBolt extends GraphiteWriterBoltBase {
    private static final Logger LOG = Logger.getLogger(AttackDetectorGraphiteWriterBolt.class);

    public AttackDetectorGraphiteWriterBolt(String graphiteServerHostname, int graphiteServerPortNumber) {
        super(graphiteServerHostname, graphiteServerPortNumber);
    }
    @Override
    public void execute(Tuple input) {
        boolean attackDetector = Boolean.parseBoolean(input.getValueByField(AttackDetectionTopology.ATTACK_DETECTOR_DETECTION_OUTPUT_FIELD).toString());
        Long attackValue = attackDetector ? -100L : 100L;
        Long timestamp = Long.parseLong(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
//        LOG.info("Sending to graphite: (attackDetector, " + attackValue + ", " + timestamp + ")");
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".attackDetectionValue", GraphiteCodec.format(attackValue), timestamp);
        super.collector.ack(input);
    }
}