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
import java.util.ArrayList;

public class GreyModelForecastingBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(GreyModelForecastingBolt.class);
    private OutputCollector collector;

    private ArrayList<Long> origSeriesOfSYN;

    public GreyModelForecastingBolt() {
        this.origSeriesOfSYN = new ArrayList<Long>();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("Received original series of TCP SYN data, apply grey model GM(1,1) to predict SYN traffic");
        int timeIndex = Integer.parseInt(tuple.getValueByField(AttackDetectionTopology.COUNTER_BOLT_TIME_INDEX_FIELD).toString());
        long actualPacketCount = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD).toString());
        long timestamp = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());

        origSeriesOfSYN.add(actualPacketCount);

        if (origSeriesOfSYN.size() <= 2) {
            collector.emit(new Values(actualPacketCount, actualPacketCount, timestamp));
            LOG.info("Emitting values: (gmVal : " + actualPacketCount + "),(actualPacketCount : " + actualPacketCount + "),(timestamp : " + timestamp);
        }
        else {
            double gmVal = calcGM(origSeriesOfSYN, timeIndex);
            collector.emit(new Values(gmVal, actualPacketCount, timestamp));
            LOG.info("Emitting values: (gmVal : " + gmVal + "),(actualPacketCount : " + actualPacketCount + "),(timestamp : " + timestamp);
        }
        collector.ack(tuple);
    }

    private double calcGM(ArrayList<Long> origSeriesOfSYN, int k) {
        double result = 0;
        int arraySize = origSeriesOfSYN.size()-1;

        /*
         * Step 2: generate 1-AGO
         */
        double[] oneAgo = new double[arraySize+1];
        double sum = 0;
        for(int i = 0; i <= arraySize; i++) {
            sum += origSeriesOfSYN.get(i);
            oneAgo[i] = sum;
        }

        /*
         * Step 3: mean generation of consecutive neighbors
         */
        double[] meanGeneration = new double[arraySize];
        for(int i = 0; i < arraySize; i++) {
            meanGeneration[i] = (oneAgo[i]+oneAgo[i+1])/2;
        }

        /*
         * Step 4: solve the model parameters a and b
         */
        // Generate matrix B
        double[][] B = new double[arraySize][2];
        for(int i = 0; i < arraySize; i++) {
            for(int j = 0; j < 2; j++) {
                if(j == 1)
                    B[i][j] = 1;
                else
                    B[i][j] = -meanGeneration[i];
            }
        }

        // Generate matrix YN
        double[][] YN = new double[arraySize][1];
        for(int i = 0; i < arraySize; i++) {
            for (int j = 0; j < 1; j++) {
                YN[i][j] = origSeriesOfSYN.get(i + 1);
            }
        }

        // Generate BT, which is the transpose of matrix B
        double[][] BT = new double[2][arraySize];
        for(int i = 0; i < 2; i++) {
            for (int j = 0; j < arraySize; j++) {
                BT[i][j] = B[j][i];
            }
        }

        // Calculate multiplication of B and BT
        double[][] BBT = new double[2][2];
        // Rows of BT
        for(int i = 0; i < 2; i++) {
            // Columns of B
            for(int j = 0; j < 2; j++) {
                // Columns of BT and Rows of B
                for(int x = 0; x < arraySize; x++){
                    BBT[i][j] += (BT[i][x]*B[x][j]);
                }
            }
        }

        // Generate the inverse matrix of BBT
        double[][] inverseBBT = new double[2][2];
        inverseBBT[0][0]=(1/(BBT[0][0]*BBT[1][1]-BBT[0][1]*BBT[1][0]))*BBT[1][1];
        inverseBBT[0][1]=(1/(BBT[0][0]*BBT[1][1]-BBT[0][1]*BBT[1][0]))*(-BBT[0][1]);
        inverseBBT[1][0]=(1/(BBT[0][0]*BBT[1][1]-BBT[0][1]*BBT[1][0]))*(-BBT[1][0]);
        inverseBBT[1][1]=(1/(BBT[0][0]*BBT[1][1]-BBT[0][1]*BBT[1][0]))*BBT[0][0];

        // Calculate multiplication of inverseBBT and BT as matrix A
        double[][] A = new double[2][arraySize];
        for(int i = 0; i < 2; i++) {
            for (int j = 0; j < arraySize; j++) {
                for(int x = 0; x < 2; x++){
                    A[i][j] += (inverseBBT[i][x]*BT[x][j]);
                }
            }
        }

        // Calculate multiplication of A and YN as matrix C
        double[][] C = new double[2][1];
        for(int i = 0; i < 2; i++) {
            for (int j = 0; j < 1; j++) {
                for(int x = 0; x < arraySize; x++){
                    C[i][j] += (A[i][x]*YN[x][j]);
                }
            }
        }

        // Get the model parameters a and b
        double a = C[0][0];
        double b = C[1][0];

        /*
         * Step 5: calculate the desired prediction output
         */
        if (!(Double.isNaN(a) && Double.isNaN(b)))
            result = (1 - Math.exp(-a)) * ((origSeriesOfSYN.get(0) - b/a) * Math.exp(-a*k));

        return result;
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
