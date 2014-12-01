package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
        collector.ack(tuple);
        if(Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.CUSUM_MODEL_SUM_OUTPUT_FIELD).toString()) > detectionThreshold) {
            collector.emit(new Values(true));
        } else {
            collector.emit(new Values(false));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.ATTACK_DETECTOR_DETECTION_OUTPUT_FIELD));
    }
}
