package com.fusion.elements;
public class StringElement implements Element<String>{
	
	private static final long serialVersionUID = 8343708711675449460L;
	private String element;
	
	public StringElement(String element){
		this.element = element;
	}
	
	public String getElement(){
		return element;
	}
	
	@Override
	public int compareTo(Element<String> other) {
		return this.element.compareTo(((StringElement)other).getElement());
	}

}
