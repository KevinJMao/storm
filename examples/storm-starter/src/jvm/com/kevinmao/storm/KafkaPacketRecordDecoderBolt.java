package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kevinmao.util.Packet;
import org.apache.log4j.Logger;

import java.util.Map;

public class KafkaPacketRecordDecoderBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(KafkaPacketRecordDecoderBolt.class);
    private OutputCollector collector;

    public KafkaPacketRecordDecoderBolt() {

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

        String recordString = new String(input.getBinary(0));
        LOG.info(recordString);

        String[] recordStringSplit = recordString.split("[\\s]", 8);

        try{
            long index = Long.parseLong(recordStringSplit[0]);
            double timestamp = Double.parseDouble(recordStringSplit[1]);
            String source_ip = recordStringSplit[2];
            String destination_ip = recordStringSplit[4];
            String protocol = recordStringSplit[5];
            int length = Integer.parseInt(recordStringSplit[6]);
            String details = recordStringSplit[7];

            Packet pkt = new Packet(index, timestamp, source_ip, destination_ip, protocol, length, details);
            collector.emit(new Values(pkt));
            collector.ack(input);
        } catch (Exception ex) {
            LOG.error("Error parsing record: " + recordString);
            LOG.error(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AttackDetectionTopology.DECODER_BOLT_PACKET_RECORD_OUTPUT_FIELD));
    }
}
