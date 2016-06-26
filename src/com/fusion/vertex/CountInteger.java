package com.fusion.vertex;

import java.io.IOException;

import com.fusion.io.OutputContext;

public class CountInteger extends AbstractVertex<Integer>{

	int nbrElement = Integer.MAX_VALUE;
	
	@Override
	public void execute(Integer line, OutputContext collector) throws IOException {
		nbrElement++;
	}
	
	@Override
	public void close(OutputContext collector){
		try {
			collector.add(nbrElement);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
