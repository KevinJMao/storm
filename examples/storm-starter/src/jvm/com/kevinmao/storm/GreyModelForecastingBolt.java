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

public class GreyModelForecastingBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(GreyModelForecastingBolt.class);
    private OutputCollector collector;
    private int dequeSize, forecastAhead;
    private GreyModelForecast forecast;
    private int greyModelSizeCounter = 0;

    public GreyModelForecastingBolt(int dequeSize, int forecastAhead) {
        this.dequeSize = dequeSize;
        this.forecastAhead = forecastAhead;
        this.forecast = new GreyModelForecast(dequeSize, forecastAhead);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        Double actualPacketCount = Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD).toString());
        long timestamp = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());

        if(greyModelSizeCounter++ > AttackDetectionTopology.GREY_MODEL_MAX_SAMPLE_COUNT) {
            forecast = new GreyModelForecast(dequeSize, forecastAhead);
            greyModelSizeCounter = 0;
        }

        forecast.update(actualPacketCount, timestamp);

        Double forecastedValue = forecast.getForecast();
        if (forecastedValue != null && !forecastedValue.isNaN() && forecastedValue >= 0.0) {
            LOG.info("Emitting (forecastedValue : " + forecastedValue + ", actualPacketCount : " + actualPacketCount +", timestamp : " + timestamp + ")");
            collector.emit(new Values(forecastedValue, actualPacketCount, timestamp));
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD,
                AttackDetectionTopology.GREY_MODEL_ACTUAL_VOLUME_OUTPUT_FIELD,
                AttackDetectionTopology.LAST_TIMESTAMP_MEASURED));
    }
}

class GreyModelForecastingGraphiteWriterBolt extends GraphiteWriterBoltBase {
    private static final Logger LOG = Logger.getLogger(GreyModelForecastingGraphiteWriterBolt.class);
    public GreyModelForecastingGraphiteWriterBolt(String graphiteServerHostname, int graphiteServerPortNumber) {
        super(graphiteServerHostname, graphiteServerPortNumber);
    }
    @Override
    public void execute(Tuple input) {
        Double greyForecastedValue = Double.parseDouble(input.getValueByField(AttackDetectionTopology.GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD).toString());
        Long timestamp = Long.parseLong(input.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
        LOG.info("Sending to graphite: (greyForecastedVolume, " + greyForecastedValue + ", " + timestamp + ")");
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".greyForecastedVolume", GraphiteCodec.format(greyForecastedValue), timestamp);
        super.collector.ack(input);
    }
}
