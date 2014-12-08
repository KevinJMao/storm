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
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;

public class GreyModelForecastingBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(GreyModelForecastingBolt.class);
    private OutputCollector collector;

    private static final double BACKGROUND_VALUE_P = 0.5;
    private ArrayDeque<Double> actualInputValues_x0;
    private ArrayDeque<Double> accumulatedSum_x1;
    private ArrayDeque<Double> forecastedAccumulatedSum_z1;
    private int maxDequeSize;
    private Integer timeIndex;

    public GreyModelForecastingBolt(int dequeLimit) {
        //The head (or first element) of the Deque is the most recent
        this.maxDequeSize = dequeLimit;
        this.actualInputValues_x0 = new ArrayDeque<Double>(dequeLimit);
        this.accumulatedSum_x1 = new ArrayDeque<Double>(dequeLimit);
        this.forecastedAccumulatedSum_z1 = new ArrayDeque<Double>(dequeLimit);
        timeIndex = 0;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public Double getLatestForecastedSum() {
        return forecastedAccumulatedSum_z1.getFirst();
    }

    private Values executeOnValuePair(Double actualPacketCount, Long timestamp) {
        actualInputValues_x0.addFirst(actualPacketCount);
//        if(actualInputValues_x0.size() > maxDequeSize) { actualInputValues_x0.removeLast(); }

        Double accumulatedSum = actualPacketCount + (accumulatedSum_x1.isEmpty() ? 0.0 : accumulatedSum_x1.getFirst());
        accumulatedSum_x1.addFirst(accumulatedSum);
//        if(accumulatedSum_x1.size() > maxDequeSize) { accumulatedSum_x1.removeLast(); }

        LOG.info("ACTUAL PACKET COUNT: " + actualPacketCount);
        LOG.info("ACTUAL PACKET COUNT ARRAY" + actualInputValues_x0);
        LOG.info("ACCUMULATED SUM ARRAY" + accumulatedSum_x1);

        //Have to bootstrap eqn7 by 1, calculate starting on the second value that comes in
        //The size of this list will subsequently be one smaller than the size of the actualInputValues list
        if(timeIndex >= 1) {
            int k = timeIndex - 1;
            Iterator<Double> iter = accumulatedSum_x1.iterator();
            double accumulatedSumDecayingAverage = ((1 - BACKGROUND_VALUE_P) * iter.next() +
                    (BACKGROUND_VALUE_P * iter.next()));

            forecastedAccumulatedSum_z1.addFirst(accumulatedSumDecayingAverage);
//            if(forecastedAccumulatedSum_z1.size() > maxDequeSize) { forecastedAccumulatedSum_z1.removeLast(); }

            LOG.info("ACCUMULATED SUM MOVING AVERAGE ARRAY: " + forecastedAccumulatedSum_z1);

            if(timeIndex >= 2) {
                double emitForecast = decayingAverageCalculation_Eq7();
                double emitActualOutputVolume = actualPacketCount;
                long lastTimestampMeasured = timestamp;

                LOG.info("EMITTING TUPLE WITH VALUES: (" + emitForecast +", "+emitActualOutputVolume+","+lastTimestampMeasured+")");
                return new Values(emitForecast, emitActualOutputVolume, lastTimestampMeasured);
            }
        }

        return null;
    }

    @Override
    public void execute(Tuple tuple) {

        Double actualPacketCount = Double.parseDouble(tuple.getValueByField(AttackDetectionTopology.COUNTER_BOLT_PACKET_COUNT_FIELD).toString());
        long timestamp = Long.parseLong(tuple.getValueByField(AttackDetectionTopology.LAST_TIMESTAMP_MEASURED).toString());
        Values result = executeOnValuePair(actualPacketCount, timestamp);

        if (result != null) {
            collector.emit(result);
        }
        timeIndex++;
        collector.ack(tuple);
    }

    private RealMatrix generateMatrix_B(){
        double[][] matrixData = new double[forecastedAccumulatedSum_z1.size()][2];
        int counter = 0;
        Iterator<Double> itr = forecastedAccumulatedSum_z1.descendingIterator();

        while(itr.hasNext() && counter < forecastedAccumulatedSum_z1.size()) {
            matrixData[counter][0] = (-1.0 * itr.next());
            matrixData[counter][1] = 1.0;
            counter++;
        }
        return MatrixUtils.createRealMatrix(matrixData);
    }

    private RealMatrix generateMatrix_Yn(){
        double[][] matrixData = new double[forecastedAccumulatedSum_z1.size()][1];

        //We're pretty much hoping that the size of the actual input value is one entry larger than the forecasted accumulation z
        int counter_x0 = 0;
        Iterator<Double> itr = actualInputValues_x0.iterator();

        while(itr.hasNext() && counter_x0 < forecastedAccumulatedSum_z1.size()) {
            matrixData[forecastedAccumulatedSum_z1.size() - counter_x0 - 1][0] = itr.next();
            counter_x0++;
        }

        //Unsure of whether to use createColumnRealMatrix or columnRowMatrix
        return MatrixUtils.createRealMatrix(matrixData);
    }


    private double decayingAverageCalculation_Eq7() {
        RealMatrix B = generateMatrix_B();
        RealMatrix Yn = generateMatrix_Yn();

        LOG.info("Matrix B is " + B.toString());
        LOG.info("Matrix Yn is " + Yn.toString());

        RealMatrix B_transpose = B.transpose();
        LOG.info("Matrix B_transpose is " + B_transpose);

        RealMatrix B_transpose__mult__B = B_transpose.multiply(B);
        LOG.info("Matrix Btrans_mult_B is " + B_transpose__mult__B);

        RealMatrix inverseOf__B__mult__B_transpose = new LUDecomposition(B_transpose__mult__B).getSolver().getInverse();
        LOG.info("Matrix inverseOf_Btrans_mult_B is " + inverseOf__B__mult__B_transpose);

        RealMatrix B_transpose__mult__Yn = B_transpose.multiply(Yn);
        LOG.info("Matrix B_transpose__mult__Yn is " + B_transpose__mult__Yn);



        //Assuming we are okay to multiply the last two components here because matrix multiplication is associative
//        RealMatrix B_transpose__mult__Yn = B_transpose.multiply(Yn);

        RealMatrix a_b_coefficients = inverseOf__B__mult__B_transpose.multiply(B_transpose__mult__Yn);

        LOG.info(("GREY MODEL COEFFICIENT MATRIX: " + a_b_coefficients.toString()));
        Double a_coeff = a_b_coefficients.getEntry(0, 0);
        Double b_coeff = a_b_coefficients.getEntry(1, 0);

        Double returnResult = cumulativeGreyForecastingEquation(a_coeff, b_coeff, actualInputValues_x0.getLast(), timeIndex);

        LOG.info("GREY MODEL RESULTS: (a : " + a_coeff + "),(b : " + b_coeff + "),(" + returnResult + ")");
        return returnResult;
    }

    private Double greyForecastingEquation(Double a, Double b, Double rawValue, Integer timeIndex) {
        assert(!a.isNaN() && !a.isInfinite() && a != null);
        assert(!b.isNaN() && !b.isInfinite() && b != null);
        assert(!rawValue.isNaN() && !rawValue.isInfinite() && rawValue != null);

        Double term1 = 1.0 - java.lang.Math.exp(a);
        Double term2 = (rawValue - (b / a));
        Double term3 = java.lang.Math.exp(-1.0 * a * timeIndex);

        LOG.info("GREY MODEL TERMS: (term1 : " + term1 + "),(term2 : " + term2 + "),(term3 : " + term3 + ")");
        return (term1 * term2 * term3);
    }

    private Double cumulativeGreyForecastingEquation(Double a, Double b, Double rawValue, Integer timeIndex) {
        assert(!a.isNaN() && !a.isInfinite() && a != null);
        assert(!b.isNaN() && !b.isInfinite() && b != null);
        assert(!rawValue.isNaN() && !rawValue.isInfinite() && rawValue != null);

        Double term1 = (rawValue - (b / a));
        Double term2 = java.lang.Math.exp(-1.0 * a * timeIndex);
        Double term3 = b / a;

        LOG.info("GREY MODEL TERMS: (term1 : " + term1 + "),(term2 : " + term2 + "),(term3 : " + term3 + ")");
        return ((term1 * term2) + term3);
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
