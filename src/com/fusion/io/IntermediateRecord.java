package com.fusion.io;

import com.fusion.elements.Element;

public class IntermediateRecord<T extends Element> implements Comparable<IntermediateRecord<T>>{

	private final T element;
	
	public IntermediateRecord(final T element) {
		this.element = element;
	}
	
	public T getElement(){
		return element;
	}

	@Override
	public int compareTo(final IntermediateRecord<T> o) {
		return this.element.compareTo(o.element);
	}
	
}
