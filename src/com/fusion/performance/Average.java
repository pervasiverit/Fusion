package com.fusion.performance;

import java.util.*;

public class Average {
  private Collection<Double> values = new ArrayList<Double>();

  public void add(double value) {
    values.add(value);
  }

  public double mean() {
    int elements = values.size();
    if (elements == 0) throw new IllegalStateException("No values");
    double sum = 0;
    for (double value : values) {
      sum += value;
    }
    return sum / elements;
  }

  public double stddev() {
    double mean = mean();
    double stddevtotal = 0;
    for (double value : values) {
      double dev = value - mean;
      stddevtotal += dev * dev;
    }
    return Math.sqrt(stddevtotal / values.size());
  }
}
