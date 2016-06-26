package com.fusion.vertex;

import java.io.IOException;

import com.fusion.io.OutputContext;
import com.fusion.utils.SerializablePredicate;

public class Filter<T extends Comparable> extends AbstractVertex<T> {

	SerializablePredicate<T> applyFunc;

	public Filter(SerializablePredicate<T> func) {
		this.applyFunc = func;
	}

	@Override
	public void execute(T line, OutputContext collector) throws IOException {
		if(applyFunc.test(line)){
			collector.add(line);
		}
	}
}