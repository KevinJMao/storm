package com.kevinmao.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import com.kevinmao.bolt.KafkaPacketRecordDecoderBolt;
import storm.kafka.KafkaSpout;
import backtype.storm.topology.TopologyBuilder;
import org.apache.log4j.Logger;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class AttackDetectionTopology {
    private static final String TOPOLOGY_NAME = "Grey Model Cumulative Sum Attack Detection Topology";
    private static final Logger LOG = Logger.getLogger(AttackDetectionTopology.class);
    private static final String ZOOKEEPER_HOSTS = "zookeeper1.kevinmao.com:2181";
    private static final String SPOUT_INPUT_KAFKA_TOPIC = "ddosdata.tovictim.text";
    private static final int SPOUT_PARALLELISM = 2;
    private static final int DECODER_BOLT_PARALLELISM = 4;

    public AttackDetectionTopology() {
    }

    public void run(Config topologyConfig) {
        StormTopology topology = buildTopology();

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, topologyConfig, topology);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            System.err.println(ex.getStackTrace());
            LOG.error(ex);
        }
    }

    private StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        String SPOUT_ID = "attk-KAFKA_SPOUT";
        String DECODER_BOLT_ID = "attk-PACKET_DECODER_BOLT";
        String COUNTER_BOLT_ID = "attk-PACKET_COUNTER_BOLT";
        String GREY_MODEL_BOLT_ID = "attk-GREY_MODEL_BOLT";
        String CUSUM_BOLT_ID = "attk-CUSUM_BOLT";
        String ATTACK_DETECTOR_BOLT_ID = "attk-ATTACK_DETECT_BOLT";
        String GRAPHITE_WRITER_BOLT_ID = "attk-GRAPHITE_WRITER_BOLT";

        //Kafka Spout Configuration
        SpoutConfig spout_conf = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOSTS),
                SPOUT_INPUT_KAFKA_TOPIC,
                "attk-kafka_spout",
                "attackDetectionTopology-id");
        builder.setSpout(SPOUT_ID, new KafkaSpout(spout_conf), SPOUT_PARALLELISM);

        //Kafka Packet Record Decoder Bolt Configuration
        KafkaPacketRecordDecoderBolt decoderBolt = new KafkaPacketRecordDecoderBolt();
        builder.setBolt(DECODER_BOLT_ID, decoderBolt, DECODER_BOLT_PARALLELISM).localOrShuffleGrouping(SPOUT_ID);

        //Packet Record Counter Bolt Configuration

        //Grey Model Forecasting Bolt Configuration

        //Cumulative Sum Aggregation Bolt Configuration

        //Attack Detector Bolt Configuration

        //Graphite Writer Bolt Configuration

        return builder.createTopology();
    }

    private Config createTopologyConfig() {
        Config config = new Config();

        return config;
    }

    public static void main(String[] args) {

    }
}
