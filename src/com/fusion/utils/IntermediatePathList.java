package com.fusion.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntermediatePathList extends ArrayList<IntermediatePathMapping> {

	public void add(Map<Integer, String> path) {
		add(new IntermediatePathMapping(path));
	}

	/**
	 * Change it to Optional Later
	 * @return
	 */
	public List<String> getTopPath() {
		if (size() == 0)
			return null;
		List<String> str = new ArrayList<>();
		for (IntermediatePathMapping mapping : this) {
			String temp = mapping.getTopPath();
			if (temp == null) {
				return null;
			}
			str.add(temp);
		}
		return str;
	}

	public static void main(String[] args) {
		IntermediatePathList pathList = new IntermediatePathList();
		Map<Integer, String> path = new HashMap<>();
		path.put(2, "xyz");
		path.put(3, "123123");
		path.put(1, "123123");
		path.put(4, "dsaadsa");
		Map<Integer, String> path1 = new HashMap<>();
		path1.put(2, "xyz");
		path1.put(3, "123123");
		path1.put(1, "123123");
		path1.put(4, "dsaadsa");
		pathList.add(new IntermediatePathMapping(path));
		pathList.add(new IntermediatePathMapping(path1));
		System.out.println(pathList.getTopPath());
		System.out.println(pathList.getTopPath());
		System.out.println(pathList.getTopPath());
		System.out.println(pathList.getTopPath());
		System.out.println(pathList.getTopPath());
	}

}
