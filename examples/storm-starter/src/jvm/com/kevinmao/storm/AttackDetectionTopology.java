package com.kevinmao.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import storm.kafka.KafkaSpout;
import backtype.storm.topology.TopologyBuilder;
import org.apache.log4j.Logger;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class AttackDetectionTopology {
    private static final String TOPOLOGY_NAME = "Grey Model Cumulative Sum Attack Detection Topology";
    private static final Logger LOG = Logger.getLogger(AttackDetectionTopology.class);

    private static final String ZOOKEEPER_HOSTS = "zookeeper1.kevinmao.com:2181";
    private static final String SPOUT_INPUT_KAFKA_TOPIC = "ddos.tovictim.text4";
    private static final int SPOUT_PARALLELISM = 4;

    private static final int DECODER_BOLT_PARALLELISM = 4;
    public static final String DECODER_BOLT_PACKET_RECORD_OUTPUT_FIELD = "packetRecord";

    private static final int COUNTER_BOLT_PARALLELISM = 1;
    private static final double COUNTER_BOLT_COUNTING_TIME_WINDOW = 1.0;
    public static final String COUNTER_BOLT_TIME_INDEX_FIELD = "timeIndex";
    public static final String COUNTER_BOLT_PACKET_COUNT_FIELD = "packetCount";

    private static final int GREY_MODEL_BOLT_PARALLELISM = 1;
    public static final String GREY_MODEL_FORECASTED_VOLUME_OUTPUT_FIELD = "forecastedVolume";
    public static final String GREY_MODEL_ACTUAL_VOLUME_OUTPUT_FIELD = "actualVolume";
    public static final int GREY_MODEL_MAX_SAMPLE_COUNT = 60;
    public static final int GREY_MODEL_LOOKAHEAD = 4;


    private static final int CUSUM_MODEL_BOLT_PARALLELISM = 1;
    public static final String CUSUM_ACTUAL_SUM_OUTPUT_FIELD = "actualCuSum";
    public static final String CUSUM_GREY_SUM_OUTPUT_FIELD = "greyCuSum";

    private static final int ATTACK_DETECTOR_BOLT_PARALLELISM = 1;
    private static final double ATTACK_DETECTOR_BOLT_DETECTION_THRESHOLD_VALUE = 1000.0;
    public static final String ATTACK_DETECTOR_DETECTION_OUTPUT_FIELD = "attackDetected";

    private static final int GRAPHITE_WRITER_BOLT_PARALLELISM = 1;
    private static final String GRAPHITE_SERVER_HOSTNAME = "monitor1.kevinmao.com";
    private static final int GRAPHITE_SERVER_PORT = 2003;

    public static final String LAST_TIMESTAMP_MEASURED = "lastTimestampSeconds";
    public static final Long TOPOLOGY_START_TIME_MILLIS = System.currentTimeMillis() - 1800000;

    public AttackDetectionTopology() {
    }

    public void run() {
        StormTopology topology = buildTopology();

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, createTopologyConfig(), topology);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            System.err.println(ex.getStackTrace());
            LOG.error(ex);
        }
    }

    private StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        String SPOUT_ID = "Kafka Spout";
        String DECODER_BOLT_ID = "Packet Decoder Bolt";
        String COUNTER_BOLT_ID = "Packet Counter Bolt";
        String COUNTER_BOLT_GRAPHITE_ID = "Packet Counter Graphite Reporting Bolt";
        String GREY_MODEL_BOLT_ID = "Grey Model Forecasting Bolt";
        String GREY_MODEL_BOLT_GRAPHITE_ID = "Grey Model Graphite Reporting Bolt";
        String CUSUM_BOLT_ID = "Cumulative Sum Aggregation Bolt";
        String CUSUM_BOLT_GRAPHITE_ID = "Cumulative Sum Graphite Reporting Bolt";
        String ATTACK_DETECTOR_BOLT_ID = "DDoS Attack Detection Bolt";
        String ATTACK_DETECTOR_BOLT_GRAPHITE_ID = "DDoS Attack Graphite Reporting Bolt";

        //Kafka Spout Configuration
        SpoutConfig spout_conf = new SpoutConfig(
                new ZkHosts(ZOOKEEPER_HOSTS),
                SPOUT_INPUT_KAFKA_TOPIC,
                "/attk-kafka_spout",
                "attackDetectionTopology-id");
        spout_conf.forceFromStart = true;
        builder.setSpout(SPOUT_ID, new KafkaSpout(spout_conf), SPOUT_PARALLELISM);

        //Kafka Packet Record Decoder Bolt Configuration
        KafkaPacketRecordDecoderBolt decoderBolt = new KafkaPacketRecordDecoderBolt();
        builder.setBolt(DECODER_BOLT_ID, decoderBolt, DECODER_BOLT_PARALLELISM).localOrShuffleGrouping(SPOUT_ID);

        //Packet Record Counter Bolt Configuration
        PacketRecordCounterBolt counterBolt = new PacketRecordCounterBolt(COUNTER_BOLT_COUNTING_TIME_WINDOW);
        builder.setBolt(COUNTER_BOLT_ID, counterBolt, COUNTER_BOLT_PARALLELISM).localOrShuffleGrouping(DECODER_BOLT_ID);

        PacketRecordCounterGraphiteWriterBolt counterBoltGraphiteWriter =
                new PacketRecordCounterGraphiteWriterBolt(GRAPHITE_SERVER_HOSTNAME, GRAPHITE_SERVER_PORT);
        builder.setBolt(COUNTER_BOLT_GRAPHITE_ID, counterBoltGraphiteWriter, GRAPHITE_WRITER_BOLT_PARALLELISM).localOrShuffleGrouping(COUNTER_BOLT_ID);

        //Grey Model Forecasting Bolt Configuration
        GreyModelForecastingBolt greyModelBolt = new GreyModelForecastingBolt(GREY_MODEL_MAX_SAMPLE_COUNT, GREY_MODEL_LOOKAHEAD);
        builder.setBolt(GREY_MODEL_BOLT_ID, greyModelBolt, GREY_MODEL_BOLT_PARALLELISM).localOrShuffleGrouping(COUNTER_BOLT_ID);

        GreyModelForecastingGraphiteWriterBolt grayModelGraphiteBolt =
                new GreyModelForecastingGraphiteWriterBolt(GRAPHITE_SERVER_HOSTNAME, GRAPHITE_SERVER_PORT);
        builder.setBolt(GREY_MODEL_BOLT_GRAPHITE_ID, grayModelGraphiteBolt, GRAPHITE_WRITER_BOLT_PARALLELISM).localOrShuffleGrouping(GREY_MODEL_BOLT_ID);

        //Cumulative Sum Aggregation Bolt Configuration
        CumulativeSumAggregationBolt cuSumBolt = new CumulativeSumAggregationBolt();
        builder.setBolt(CUSUM_BOLT_ID, cuSumBolt, CUSUM_MODEL_BOLT_PARALLELISM).localOrShuffleGrouping(GREY_MODEL_BOLT_ID);

        CumulativeSumAggregationGraphiteWriterBolt cuSumGraphiteBolt =
                new CumulativeSumAggregationGraphiteWriterBolt(GRAPHITE_SERVER_HOSTNAME, GRAPHITE_SERVER_PORT);
        builder.setBolt(CUSUM_BOLT_GRAPHITE_ID, cuSumGraphiteBolt, GRAPHITE_WRITER_BOLT_PARALLELISM).localOrShuffleGrouping(CUSUM_BOLT_ID);

        //Attack Detector Bolt Configuration
        AttackDetectorBolt detectorBolt = new AttackDetectorBolt(ATTACK_DETECTOR_BOLT_DETECTION_THRESHOLD_VALUE);
        builder.setBolt(ATTACK_DETECTOR_BOLT_ID, detectorBolt, ATTACK_DETECTOR_BOLT_PARALLELISM).localOrShuffleGrouping(CUSUM_BOLT_ID);

        AttackDetectorGraphiteWriterBolt detectorGraphiteBolt =
                new AttackDetectorGraphiteWriterBolt(GRAPHITE_SERVER_HOSTNAME, GRAPHITE_SERVER_PORT);
        builder.setBolt(ATTACK_DETECTOR_BOLT_GRAPHITE_ID, detectorGraphiteBolt, GRAPHITE_WRITER_BOLT_PARALLELISM).localOrShuffleGrouping(ATTACK_DETECTOR_BOLT_ID);

        return builder.createTopology();
    }

    private Config createTopologyConfig() {
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(4);
        config.setMaxSpoutPending(1000);
        config.setMessageTimeoutSecs(60);
        config.setNumAckers(0);
        config.setMaxTaskParallelism(50);

        return config;
    }

    public static void main(String[] args) {
        AttackDetectionTopology topology = new AttackDetectionTopology();
        topology.run();
    }
}
