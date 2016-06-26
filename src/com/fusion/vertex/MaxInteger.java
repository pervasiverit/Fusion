package com.fusion.vertex;

import java.io.IOException;

import com.fusion.io.OutputContext;

public class MaxInteger extends AbstractVertex<Integer>{
	
	int total = Integer.MIN_VALUE;
	
	@Override
	public void execute(Integer line, OutputContext collector) throws IOException {
		total= Math.max(line, total);
	}
	
	@Override
	public void close(OutputContext collector){
		try {
			collector.add(total);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}