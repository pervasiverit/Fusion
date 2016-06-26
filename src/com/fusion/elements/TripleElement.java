package com.fusion.elements;

import com.fusion.utils.Triple;

public class TripleElement implements Element<Triple>{
	
	Triple triple;

	public TripleElement(Triple triple){
		this.triple = triple;
	}
	
	@Override
	public Triple getElement() {
		return triple;
	}

	@Override
	public int compareTo(Element<Triple> o) {
		return 0;
	}
	
	
	public static void main(String[] args) {
		Triple<Integer,Integer,Double> triple = new Triple<>(10,1,1.0);
		TripleElement element = new TripleElement(triple);
		System.out.println(element.getElement().getFirst());
		
	}

}
