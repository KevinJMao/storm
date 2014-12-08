package com.kevinmao.storm;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Iterator;

public class GreyModelForecast implements Serializable{
    private ArrayDeque<Double> actualInputValues_x0;
    private ArrayDeque<Double> accumulatedSum_x1;
    private ArrayDeque<Double> forecastedAccumulatedSum_z1;
    private static final double BACKGROUND_VALUE_P = 0.5;
    private Integer timeIndex;
    private Integer forecastAhead;
    private static final Logger LOG = Logger.getLogger(GreyModelForecast.class);


    public GreyModelForecast(int dequeSize, int forecastAhead) {
        this.forecastAhead = forecastAhead;
        //The head (or first element) of the Deque is the most recent
        this.actualInputValues_x0 = new ArrayDeque<Double>(dequeSize);
        this.accumulatedSum_x1 = new ArrayDeque<Double>(dequeSize);
        this.forecastedAccumulatedSum_z1 = new ArrayDeque<Double>(dequeSize);
        timeIndex = 0;
    }

    public Double getForecast() {
        //If we haven't compiled enough data to make an accurate prediction, don't give an answer
        if(timeIndex >= 3) {
            return decayingAverageCalculation_Eq7();
        }
        else {
            return Double.NaN;
        }
    }

    public void update(Double actualPacketCount, Long timestamp) {
        actualInputValues_x0.addFirst(actualPacketCount);
//        if(actualInputValues_x0.size() > maxDequeSize) { actualInputValues_x0.removeLast(); }

        Double accumulatedSum = actualPacketCount + (accumulatedSum_x1.isEmpty() ? 0.0 : accumulatedSum_x1.getFirst());
        accumulatedSum_x1.addFirst(accumulatedSum);
//        if(accumulatedSum_x1.size() > maxDequeSize) { accumulatedSum_x1.removeLast(); }

        LOG.debug("ACTUAL PACKET COUNT: " + actualPacketCount);
        LOG.debug("ACTUAL PACKET COUNT ARRAY" + actualInputValues_x0);
        LOG.debug("ACCUMULATED SUM ARRAY" + accumulatedSum_x1);

        //Have to bootstrap eqn7 by 1, calculate starting on the second value that comes in
        //The size of this list will subsequently be one smaller than the size of the actualInputValues list
        if(timeIndex >= 1) {
            int k = timeIndex - 1;
            Iterator<Double> iter = accumulatedSum_x1.iterator();
            double accumulatedSumDecayingAverage = ((1 - BACKGROUND_VALUE_P) * iter.next() +
                    (BACKGROUND_VALUE_P * iter.next()));

            forecastedAccumulatedSum_z1.addFirst(accumulatedSumDecayingAverage);
//            if(forecastedAccumulatedSum_z1.size() > maxDequeSize) { forecastedAccumulatedSum_z1.removeLast(); }

            LOG.debug("ACCUMULATED SUM MOVING AVERAGE ARRAY: " + forecastedAccumulatedSum_z1);
        }
        timeIndex++;
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

        LOG.debug("Matrix B is " + B.toString());
        LOG.debug("Matrix Yn is " + Yn.toString());

        RealMatrix B_transpose = B.transpose();
        LOG.debug("Matrix B_transpose is " + B_transpose);

        RealMatrix B_transpose__mult__B = B_transpose.multiply(B);
        LOG.debug("Matrix Btrans_mult_B is " + B_transpose__mult__B);

        RealMatrix inverseOf__B__mult__B_transpose = new LUDecomposition(B_transpose__mult__B).getSolver().getInverse();
        LOG.debug("Matrix inverseOf_Btrans_mult_B is " + inverseOf__B__mult__B_transpose);

        RealMatrix B_transpose__mult__Yn = B_transpose.multiply(Yn);
        LOG.debug("Matrix B_transpose__mult__Yn is " + B_transpose__mult__Yn);



        //Assuming we are okay to multiply the last two components here because matrix multiplication is associative
//        RealMatrix B_transpose__mult__Yn = B_transpose.multiply(Yn);

        RealMatrix a_b_coefficients = inverseOf__B__mult__B_transpose.multiply(B_transpose__mult__Yn);

        LOG.debug(("GREY MODEL COEFFICIENT MATRIX: " + a_b_coefficients.toString()));
        Double a_coeff = a_b_coefficients.getEntry(0, 0);
        Double b_coeff = a_b_coefficients.getEntry(1, 0);

        Double returnResult = greyForecastingEquation(a_coeff, b_coeff, actualInputValues_x0.getLast(), timeIndex);

        LOG.debug("GREY MODEL RESULTS: (a : " + a_coeff + "),(b : " + b_coeff + "),(" + returnResult + ")");
        return returnResult;
    }

    private Double greyForecastingEquation(Double a, Double b, Double rawValue, Integer timeIndex) {
        assert(!a.isNaN() && !a.isInfinite() && a != null);
        assert(!b.isNaN() && !b.isInfinite() && b != null);
        assert(!rawValue.isNaN() && !rawValue.isInfinite() && rawValue != null);

        Double term1 = 1.0 - java.lang.Math.exp(a);
        Double term2 = (rawValue - (b / a));
        Double term3 = java.lang.Math.exp(-1.0 * a * (timeIndex + forecastAhead));

        LOG.debug("GREY MODEL TERMS: (term1 : " + term1 + "),(term2 : " + term2 + "),(term3 : " + term3 + ")");
        return (term1 * term2 * term3);
    }

    private Double cumulativeGreyForecastingEquation(Double a, Double b, Double rawValue, Integer timeIndex) {
        assert(!a.isNaN() && !a.isInfinite() && a != null);
        assert(!b.isNaN() && !b.isInfinite() && b != null);
        assert(!rawValue.isNaN() && !rawValue.isInfinite() && rawValue != null);

        Double term1 = (rawValue - (b / a));
        Double term2 = java.lang.Math.exp(-1.0 * a * (timeIndex + forecastAhead));
        Double term3 = b / a;

        LOG.debug("GREY MODEL TERMS: (term1 : " + term1 + "),(term2 : " + term2 + "),(term3 : " + term3 + ")");
        return ((term1 * term2) + term3);
    }

}
