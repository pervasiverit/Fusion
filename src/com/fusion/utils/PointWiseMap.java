package com.fusion.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import scala.collection.immutable.Stream;

public class PointWiseMap extends HashMap<ActorRef, IntermediatePathList> {

	public void addCompleted(ActorRef actorRef, Map<Integer, String> path) {
		if (containsKey(actorRef)) {
			IntermediatePathList paths = get(actorRef);
			paths.add(path);
			put(actorRef, paths);
		} else {
			IntermediatePathList paths = new IntermediatePathList();
			paths.add(path);
			put(actorRef, paths);
		}
	}

	public Optional<Map<ActorRef, List<String>>> getMapping() {
		Map<ActorRef, List<String>> map = new HashMap<>();
		for (Map.Entry<ActorRef, IntermediatePathList> e : entrySet()) {
			List<String> list = e.getValue().getTopPath();
			if (list == null) {
				return Optional.empty();
			}
			map.put(e.getKey(), list);
		}
		return Optional.of(map);
	}

	public void cleanUp(PointWiseMap current) {
		PointWiseMap pointWiseMap = new PointWiseMap();
		for (Map.Entry<ActorRef, IntermediatePathList> e : entrySet()) {
			if(!(e.getValue().size() == 0)){
				pointWiseMap.put(e.getKey(), e.getValue());
			}
		}
		current = pointWiseMap;
	}

}
