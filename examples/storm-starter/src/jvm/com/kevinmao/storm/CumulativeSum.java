package com.kevinmao.storm;

import org.apache.commons.math3.stat.descriptive.moment.Variance;

import java.io.Serializable;

public class CumulativeSum implements Serializable {

    private Double lambda;
    private Double alpha;

    // Values that are updated each time a new sample arrives
    private Double exponentiallyWeightedAverage;
    private Double cumulativeSum;
    private Variance runningVariance;

    public CumulativeSum(Double initialValue, Double alpha, Double lambda) {
        this.exponentiallyWeightedAverage = initialValue;
        this.alpha = alpha;
        this.lambda = lambda;
        this.cumulativeSum = 0.0;
        this.runningVariance = new Variance();
        this.runningVariance.increment(initialValue);
    }

    public Double update(Double nextValue) {
        //Exponentially Weight Average of the series of values
        //Initial value is first value that comes in
        exponentiallyWeightedAverage = (lambda * exponentiallyWeightedAverage) + ((1 - lambda) * nextValue);

        //Running variance of the series of values
        runningVariance.increment(nextValue);

        //Cumulative sum
        double cusum_termA = (alpha / runningVariance.getResult()) * exponentiallyWeightedAverage;
        double cusum_termB = (nextValue - (alpha / 2) * exponentiallyWeightedAverage);

        cumulativeSum = cumulativeSum + (cusum_termA * cusum_termB);
        return cumulativeSum;
    }

    public Double getCumulativeSum() {
        return cumulativeSum;
    }
}
