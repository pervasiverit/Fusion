package com.fusion.elements;

public class IntegerElement implements Element<Integer>{

	private Integer element;
	
	public IntegerElement(int element){
		this.element = element;
	}

	@Override
	public int compareTo(Element<Integer> o) {
		return this.element.compareTo(((IntegerElement)o).getElement());
	}

	@Override
	public Integer getElement() {
		return element;
	}

}
