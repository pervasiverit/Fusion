package com.fusion.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class IntermediatePathMapping extends TreeMap<Integer, String>{

	public IntermediatePathMapping(Map<Integer, String> path) {
		putAll(path);
	}

	public String getTopPath() {
		if(size()==0) return null;
		String temp =  this.firstEntry().getValue();
		this.remove(this.firstEntry().getKey());
		return temp;
	}
	
	public static void main(String[] args) {
		Map<Integer,String> path = new HashMap<>();
		path.put(2, "xyz");
		path.put(3, "123123");
		path.put(1, "123123");
		path.put(4, "dsaadsa");
		IntermediatePathMapping mapping = new IntermediatePathMapping(path);
		mapping.toString();
		System.out.println(mapping.getTopPath().equals("123123"));
		System.out.println(mapping.getTopPath().equals("xyz"));
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

}
