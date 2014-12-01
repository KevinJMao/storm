package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;

public class CumulativeSumAggregationBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(CumulativeSumAggregationBolt.class);
    private OutputCollector collector;

    private final double ALPHA = 0.5;

    private ArrayList<Long> origSeriesOfSYN;
    private ArrayList<Double> grayModelForecastedOutput;

    public CumulativeSumAggregationBolt(){
        this.origSeriesOfSYN = new ArrayList<Long>();
        this.grayModelForecastedOutput = new ArrayList<Double>();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("Apply CUSUM algorithm to detect SYN flooding attack");

        long actualPacketCount = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.GREY_MODEL_ACTUAL_VOLUME_OUTPUT_FIELD).toString());
        origSeriesOfSYN.add(actualPacketCount);

        double grayForecastedCount = Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD).toString());
        grayModelForecastedOutput.add(grayForecastedCount);

        collector.emit(new Values(calcCUSUM(origSeriesOfSYN, grayModelForecastedOutput)));
        collector.ack(tuple);
    }

    private double calcCUSUM(ArrayList<Long> origSeriesOfSYN, ArrayList<Double> grayModelForecastedOutput) {
        double fn = 0;
        int size = origSeriesOfSYN.size();

        double variance = calcVariance(origSeriesOfSYN);

        if (variance != 0){
            fn = origSeriesOfSYN.get(0);
            for (int i = 1; i < size; i++) {
                double tmp = origSeriesOfSYN.get(i) - grayModelForecastedOutput.get(i-1) - ALPHA/2 * grayModelForecastedOutput.get(i-1);
                fn += ((ALPHA * grayModelForecastedOutput.get(i-1)/variance) * tmp);
            }
        }

        return fn;
    }

    private double calcVariance(ArrayList<Long> origSeriesOfSYN) {
        double variance = 0, sum = 0, avg = 0, tmp = 0;
        int size = origSeriesOfSYN.size();

        if (!origSeriesOfSYN.isEmpty()) {
            // Calculate sum
            for(int i = 0; i < size; i++) {
                sum += origSeriesOfSYN.get(i);
            }
            // Calculate average
            avg = sum / size;

            // Calculate variance
            for(int i = 0; i < size; i++) {
                double diff = origSeriesOfSYN.get(i) - avg;
                tmp += (diff * diff);
            }
            variance = tmp / size;
        }

        return variance;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.CUSUM_MODEL_SUM_OUTPUT_FIELD));
    }
}

class CumulativeSumAggregationGraphiteWriterBolt extends GraphiteWriterBoltBase {
    public CumulativeSumAggregationGraphiteWriterBolt(String graphiteServerHostname, int graphiteServerPortNumber) {
        super(graphiteServerHostname, graphiteServerPortNumber);
    }
    @Override
    public void execute(Tuple input) {

    }
}
