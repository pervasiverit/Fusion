package com.fusion.utils;

import java.io.Serializable;

public class Triple<X,Y,Z> implements Serializable{
	
	X element1;
	Y element2;
	Z element3;
	
	public Triple(X ele1, Y ele2 , Z ele3){
		this.element1 = ele1;
		this.element2 = ele2;
		this.element3 = ele3;
	}
	
	public X getFirst(){
		return element1;
	}
	
	public Y getSecond() {
		return element2;
	}
	
	public Z getThird(){
		return element3;
	}
	
}
