package com.kevinmao.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kevinmao.graphite.GraphiteCodec;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils.*;

import java.lang.Math;
import java.util.Map;
import java.util.ArrayList;

public class GreyModelForecastingBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(GreyModelForecastingBolt.class);
    private OutputCollector collector;

    private static final double BACKGROUND_VALUE_P = 0.5;
    private ArrayList<Double> actualInputValues_x0;
    private ArrayList<Double> accumulatedSum_x1;
    private ArrayList<Double> forecastedAccumulatedSum_z1;
    private Integer timeIndex;

    public GreyModelForecastingBolt() {
        this.actualInputValues_x0 = new ArrayList<Double>();
        this.accumulatedSum_x1 = new ArrayList<Double>();
        this.forecastedAccumulatedSum_z1 = new ArrayList<Double>();
        timeIndex = 0;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        long actualPacketCount = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD).toString());
        long timestamp = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());

        actualInputValues_x0.add(Double.parseDouble(Long.toString(actualPacketCount)));
        accumulatedSum_x1.add((new Long(actualPacketCount)).doubleValue() + (accumulatedSum_x1.isEmpty() ? 0.0 : accumulatedSum_x1.get(accumulatedSum_x1.size() - 1)));

        //Have to bootstrap eqn7 by 1, calculate starting on the second value that comes in
        //The size of this list will subsequently be one smaller than the size of the actualInputValues list
        if(timeIndex >= 1) {
            int k = timeIndex - 1;
            double accumulatedSumDecayingAverage = (BACKGROUND_VALUE_P * accumulatedSum_x1.get(k)) +
                    ((1 - BACKGROUND_VALUE_P) * accumulatedSum_x1.get(k + 1));

            forecastedAccumulatedSum_z1.add(accumulatedSumDecayingAverage);

            if(timeIndex >= 2) {
                double emitForecast = decayingAverageCalculation_Eq7();
                long emitActualOutputVolume = actualPacketCount;
                long lastTimestampMeasured = timestamp;

                collector.emit(new Values(emitForecast, emitActualOutputVolume, lastTimestampMeasured));
            }
        }
        timeIndex++;
        collector.ack(tuple);
    }

    private RealMatrix generateMatrix_B(){
        double[][] matrixData = new double[forecastedAccumulatedSum_z1.size()][2];
        int counter = 0;
        for(Double z : forecastedAccumulatedSum_z1) {
            matrixData[counter][0] = z;
            matrixData[counter][1] = 1.0;
            counter++;
        }
        return MatrixUtils.createRealMatrix(matrixData);
    }

    private RealMatrix generateMatrix_Yn(){
        double[] matrixData = new double[forecastedAccumulatedSum_z1.size()];

        //We're pretty much hoping that the size of the actual input value is one entry larger than the forecasted accumulation z
        for(int i = 1; i < actualInputValues_x0.size(); i++) {
            matrixData[i - 1] = actualInputValues_x0.get(i);
        }
        //Unsure of whether to use createColumnRealMatrix or columnRowMatrix
        return MatrixUtils.createColumnRealMatrix(matrixData);
    }


    private double decayingAverageCalculation_Eq7() {
        RealMatrix B = generateMatrix_B();
        RealMatrix Yn = generateMatrix_Yn();

//        LOG.info("Matrix B is " + B.toString());
//        LOG.info("Matrix Yn is " + Yn.toString());

        RealMatrix B_transpose = B.transpose();

        RealMatrix B_transpose__mult__B = B_transpose.multiply(B);
        RealMatrix inverseOf__B__mult__B_transpose = new LUDecomposition(B_transpose__mult__B).getSolver().getInverse();

        //Assuming we are okay to multiply the last two components here because matrix multiplication is associative
        RealMatrix B_transpose__mult__Yn = B_transpose.multiply(Yn);

        RealMatrix a_b_coefficients = inverseOf__B__mult__B_transpose.multiply(B_transpose__mult__Yn);

//        LOG.info(("GREY MODEL COEFFICIENT MATRIX: " + a_b_coefficients.toString()));
        Double a_coeff = a_b_coefficients.getEntry(0, 0);
        Double b_coeff = a_b_coefficients.getEntry(1, 0);

        Double returnResult = Math.abs(greyForecastingEquation(a_coeff, b_coeff, actualInputValues_x0.get(0), timeIndex));

        LOG.info("GREY MODEL RESULTS: (a : " + a_coeff + "),(b : " + b_coeff + "),(" + returnResult + ")");
        return returnResult;
    }

    private Double greyForecastingEquation(Double a, Double b, Double rawValue, Integer timeIndex) {
        assert(!a.isNaN() && !a.isInfinite() && a != null);
        assert(!b.isNaN() && !b.isInfinite() && b != null);
        assert(!rawValue.isNaN() && !rawValue.isInfinite() && rawValue != null);

        Double term1 = 1.0 - java.lang.Math.exp(a * -1.0);
        Double term2 = (rawValue - (b / a));
        Double term3 = java.lang.Math.exp(-1.0 * a * timeIndex);

//        LOG.info("GREY MODEL TERMS: (term1 : " + term1 + "),(term2 : " + term2 + "),(term3 : " + term3 + ")");
        return (term1 * term2 * term3);
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
//        LOG.info("Sending to graphite: (greyForecastedVolume, " + greyForecastedValue + ", " + timestamp + ")");
        super.sendToGraphite(super.GRAPHITE_PREFIX + ".greyForecastedVolume", GraphiteCodec.format(greyForecastedValue), timestamp);
        super.collector.ack(input);
    }
}
