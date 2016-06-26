package com.fusion.vertex;

import java.io.IOException;

import com.fusion.elements.IntegerElement;
import com.fusion.io.OutputContext;

public class SumInteger extends AbstractVertex<Integer>{
	
	int total = 0;
	
	@Override
	public void execute(Integer line, OutputContext collector) throws IOException {
		total += line;
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
