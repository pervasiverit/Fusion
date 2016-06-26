package com.fusion.elements;

import java.math.BigInteger;

public class BigIntegerElement implements Element<BigInteger> {

	
	private static final long serialVersionUID = 8343708711675449460L;
	private BigInteger element;
	
	public BigIntegerElement(BigInteger element){
		this.element = element;
	}
	
	public BigInteger getElement(){
		return element;
	}
	
	@Override
	public int compareTo(Element<BigInteger> other) {
		return this.element.compareTo(((BigIntegerElement)other).getElement());
	}
	
	@Override
	public String toString(){
		return element.toString();
	}
}
