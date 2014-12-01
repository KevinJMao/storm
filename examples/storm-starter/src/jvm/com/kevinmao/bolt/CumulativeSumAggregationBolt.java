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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CumulativeSumAggregationBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(CumulativeSumAggregationBolt.class);
    private OutputCollector collector;

    private static final ArrayList<Long> DEFAULT_EMPTY_LONG_ARRAY = new ArrayList<Long>();
    private static final ArrayList<Double> DEFAULT_EMPTY_DOUBLE_ARRAY = new ArrayList<Double>();
    private ArrayList<Long> origSeriesOfSYN;
    private ArrayList<Double> grayModelForecastedOutput;

    public CumulativeSumAggregationBolt(){
        this(DEFAULT_EMPTY_LONG_ARRAY, DEFAULT_EMPTY_DOUBLE_ARRAY);
    }

    public CumulativeSumAggregationBolt(ArrayList<Long> origSeriesOfSYN, ArrayList<Double> grayModelForecastedOutput) {
        this.origSeriesOfSYN = origSeriesOfSYN;
        this.grayModelForecastedOutput = grayModelForecastedOutput;

        /*
        this.origSeriesOfSYN = new ArrayList<Long>();
        this.grayModelForecastedOutput = new ArrayList<Double>();
         */
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("Apply CUSUM algorithm to detect SYN flooding attack");

        long actualPacketCount = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.GRAY_MODEL_ACTUAL_VOLUME_OUTPUT_FIELD).toString());
        origSeriesOfSYN.add(actualPacketCount);

        double grayForecastedCount = Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD).toString());
        grayModelForecastedOutput.add(grayForecastedCount);

        calcCUSUM(origSeriesOfSYN, grayModelForecastedOutput);
    }

    private double calcCUSUM(ArrayList<Long> origSeriesOfSYN, ArrayList<Double> grayModelForecastedOutput) {
        double fn = 0;

        

        return fn;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.CUSUM_MODEL_SUM_OUTPUT_FIELD));
    }
}
