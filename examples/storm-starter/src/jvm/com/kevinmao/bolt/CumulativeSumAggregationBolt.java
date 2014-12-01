package com.kevinmao.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.kevinmao.topology.AttackDetectionTopology;
import org.apache.log4j.Logger;
import storm.starter.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;

public class CumulativeSumAggregationBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(CumulativeSumAggregationBolt.class);
    private OutputCollector collector;

    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 60;

    private final int emitFrequencyInSeconds;

    public CumulativeSumAggregationBolt(){
        this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public CumulativeSumAggregationBolt(int emitFrequencyInSeconds) {
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.CUSUM_MODEL_SUM_OUTPUT_FIELD));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }

    @Override
    public void cleanup(){

    }
}
