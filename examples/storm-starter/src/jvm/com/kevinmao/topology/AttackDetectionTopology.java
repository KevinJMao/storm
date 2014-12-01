package com.kevinmao.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import com.kevinmao.bolt.*;
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
    public static final String DECODER_BOLT_PACKET_RECORD_OUTPUT_FIELD = "packetRecord";

    private static final int COUNTER_BOLT_PARALLELISM = 1;
    private static final double COUNTER_BOLT_COUNTING_TIME_WINDOW = 10.0;
    public static final String COUNTER_BOLT_TIME_INDEX_FIELD = "timeIndex";
    public static final String COUNTER_BOLT_PACKET_COUNT_FIELD = "packetCount";

    private static final int GREY_MODEL_BOLT_PARALLELISM = 1;
    public static final String GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD = "forecastedVolume";

    private static final int CUSUM_MODEL_BOLT_PARALLELISM = 1;
    public static final String CUSUM_MODEL_SUM_OUTPUT_FIELD = "totalSum";

    private static final int ATTACK_DETECTOR_BOLT_PARALLELISM = 1;
    public static final String ATTACK_DETECTOR_DETECTION_OUTPUT_FIELD = "attackDetected";

    private static final int GRAPHITE_WRITER_BOLT_PARALLELISM = 1;
    private static final String GRAPHITE_SERVER_HOSTNAME = "monitor1.kevinmao.com";
    private static final int GRAPHITE_SERVER_PORT = 2003;

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
        PacketRecordCounterBolt counterBolt = new PacketRecordCounterBolt(COUNTER_BOLT_COUNTING_TIME_WINDOW);
        builder.setBolt(COUNTER_BOLT_ID, counterBolt, COUNTER_BOLT_PARALLELISM).localOrShuffleGrouping(DECODER_BOLT_ID);

        //Grey Model Forecasting Bolt Configuration
        GrayModelForecastingBolt greyModelBolt = new GrayModelForecastingBolt();
        builder.setBolt(GREY_MODEL_BOLT_ID, greyModelBolt, GREY_MODEL_BOLT_PARALLELISM).localOrShuffleGrouping(COUNTER_BOLT_ID);

        //Cumulative Sum Aggregation Bolt Configuration
        CumulativeSumAggregationBolt cuSumBolt = new CumulativeSumAggregationBolt();
        builder.setBolt(CUSUM_BOLT_ID, cuSumBolt, CUSUM_MODEL_BOLT_PARALLELISM).localOrShuffleGrouping(GREY_MODEL_BOLT_ID);

        //Attack Detector Bolt Configuration
        AttackDetectorBolt detectorBolt = new AttackDetectorBolt();
        builder.setBolt(ATTACK_DETECTOR_BOLT_ID, detectorBolt, ATTACK_DETECTOR_BOLT_PARALLELISM).localOrShuffleGrouping(CUSUM_BOLT_ID);

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
