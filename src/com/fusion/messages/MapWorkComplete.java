package com.fusion.messages;

import java.util.Map;

import akka.actor.ActorRef;

public class MapWorkComplete extends WorkComplete{
	
	public MapWorkComplete(final ActorRef actorRef, 
			final Map<Integer, String> paths, final String taskId) {
		super(actorRef, paths, taskId);
	}

}
