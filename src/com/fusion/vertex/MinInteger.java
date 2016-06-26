package com.fusion.vertex;

import java.io.IOException;

import com.fusion.io.OutputContext;

public class MinInteger extends AbstractVertex<Integer>{

	int minElement = Integer.MAX_VALUE;
	
	@Override
	public void execute(Integer line, OutputContext collector) throws IOException {
		minElement= Math.min(minElement, line);
	}
	
	@Override
	public void close(OutputContext collector){
		try {
			collector.add(minElement);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
