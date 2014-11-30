package com.kevinmao.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kevinmao.topology.AttackDetectionTopology;
import com.kevinmao.util.Packet;
import org.apache.log4j.Logger;

import java.util.Map;

public class GraphiteWriterBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(GraphiteWriterBolt.class);
    private OutputCollector collector;

    public GraphiteWriterBolt() {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
