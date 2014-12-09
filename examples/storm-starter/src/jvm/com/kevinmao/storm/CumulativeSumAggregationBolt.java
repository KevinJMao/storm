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

public class CumulativeSumAggregationBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(CumulativeSumAggregationBolt.class);
    private OutputCollector collector;

    private static final Double ALPHA = 0.6;
    private static final Double LAMBDA = 0.5;

    private CumulativeSum actualPacketCount_CUSUM;
    private CumulativeSum greyForecasted_CUSUM;


    public CumulativeSumAggregationBolt(){
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("Apply CUSUM algorithm to detect SYN flooding attack");

        Double actualPacketCount = Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.GREY_MODEL_ACTUAL_VOLUME_OUTPUT_FIELD).toString());

        Double greyForecastedCount = Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD).toString());

        Long timestamp = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());

        if(actualPacketCount_CUSUM == null) {
            actualPacketCount_CUSUM = new CumulativeSum(actualPacketCount, ALPHA, LAMBDA);
        } else {
            if(actualPacketCount >= 0.0 && !actualPacketCount.isNaN()) {
                actualPacketCount_CUSUM.update(actualPacketCount);
            } else {
                LOG.warn("Received invalid actual packet count value.");
            }
        }

        if(greyForecasted_CUSUM == null) {
            greyForecasted_CUSUM = new CumulativeSum(greyForecastedCount, ALPHA, LAMBDA);
        } else {
            if(greyForecastedCount >= 0.0 && !greyForecastedCount.isNaN()) {
                greyForecasted_CUSUM.update(greyForecastedCount);
            } else {
                LOG.warn("Received invalid grey forecasted packet count value.");
            }
        }

        collector.emit(new Values(actualPacketCount_CUSUM.getCumulativeSum(),
                greyForecasted_CUSUM.getCumulativeSum(),
                timestamp));
        LOG.info("Emitting values: (actual count CUSUM : " + actualPacketCount_CUSUM.getCumulativeSum() +
                "),(grey count CUSUM : " + greyForecasted_CUSUM.getCumulativeSum() +
                "),(timestamp : " + timestamp + ")");

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.CUSUM_ACTUAL_SUM_OUTPUT_FIELD,
                AttackDetectionTopology.CUSUM_GREY_SUM_OUTPUT_FIELD,
                AttackDetectionTopology.LAST_TIMESTAMP_MEASURED));
    }
}

class CumulativeSumAggregationGraphiteWriterBolt extends GraphiteWriterBoltBase {
    private static final Logger LOG = Logger.getLogger(CumulativeSumAggregationGraphiteWriterBolt.class);

    public CumulativeSumAggregationGraphiteWriterBolt(String graphiteServerHostname, int graphiteServerPortNumber) {
        super(graphiteServerHostname, graphiteServerPortNumber);
    }
    @Override
    public void execute(Tuple input) {
        Double cumulativeSumActual = Double.parseDouble(input.getValueByField(AttackDetectionTopology.CUSUM_ACTUAL_SUM_OUTPUT_FIELD).toString());
        Double cumulativeSumForecast = Double.parseDouble(input.getValueByField(AttackDetectionTopology.CUSUM_GREY_SUM_OUTPUT_FIELD).toString());

        Long timestamp = Long.parseLong(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
        LOG.info("Sending to graphite: (cumulativeSumActualValues : " + cumulativeSumActual + ", " +
                "cumulativeSumGreyValues : " + cumulativeSumForecast + ", " + timestamp + ")");
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".cumulativeSumActualValues", GraphiteCodec.format(cumulativeSumActual), timestamp);
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".cumulativeSumGreyValues", GraphiteCodec.format(cumulativeSumForecast), timestamp);

        super.collector.ack(input);
    }
}
