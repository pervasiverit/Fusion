package com.fusion.performance;

public class PerformanceHarness {
	  public Average calculatePerf(PerformanceChecker check, int runs) {
	    Average avg = new Average();
	    // first we warm up the hotspot compiler
	    check.start(); check.start();
	    for(int i=0; i < runs; i++) {
	      long count = check.start();
	      avg.add(count);
	    }
	    return avg;
	  }
	}