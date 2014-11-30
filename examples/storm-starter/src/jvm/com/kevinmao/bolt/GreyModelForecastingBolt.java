package com.kevinmao.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

public class GreyModelForecastingBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(TextPcapDecoderBolt.class);
    private OutputCollector collector;

    //private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 60;

    //private final int emitFrequencyInSeconds;
    private static final ArrayList<Double> DEFAULT_EMPTY_INT_ARRAY = new ArrayList<Double>();
    private static final int DEFAULT_K = 0;

    private ArrayList<Double> origSeriesOfSYN;
    private int k;

    public GreyModelForecastingBolt(){ this(DEFAULT_EMPTY_INT_ARRAY, DEFAULT_K); }

    public GreyModelForecastingBolt(ArrayList<Double> origSeriesOfSYN, int k) {
        this.origSeriesOfSYN = origSeriesOfSYN;
        this.k = k;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        if(TupleHelpers.isTickTuple(tuple)) {
            LOG.debug("Received original series of TCP SYN data, apply grey model GM(1,1)");
            calcGM(origSeriesOfSYN, k);
        }
        else {

        }
    }

    private double calcGM(ArrayList<Double> origSeriesOfSYN, int k) {
        double result = 0;
        int arraySize = origSeriesOfSYN.size()-1;

        /*
         * Step 2: generating 1-AGO
         */
        ArrayList<Double> oneAgo = new ArrayList<Double>(arraySize+1);
        double sum = 0;
        for(int i = 0; i < arraySize+1; i++) {
            sum += origSeriesOfSYN.get(i);
            oneAgo.add(sum);
        }

        /*
         * Step 3: mean generation of consecutive neighbors
         */
        ArrayList<Double> meanGeneration = new ArrayList<Double>(arraySize);
        for(int i = 0; i < arraySize; i++) {
            meanGeneration.set(i, (oneAgo.get(i)+oneAgo.get(i+1))/2);
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
                    B[i][j] = -meanGeneration.get(i);
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
        result = (1 - Math.exp(-a)) * ((origSeriesOfSYN.get(0) - b/a) * Math.exp(-a*k));

        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("all", "the", "fields"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }

    @Override
    public void cleanup(){

    }
}
