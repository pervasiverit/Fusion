package com.fusion.messages;

import java.util.Map;

import akka.actor.ActorRef;

public class ReduceWorkComplete extends WorkComplete{

	public ReduceWorkComplete(ActorRef actorRef, Map<Integer, String> paths, String taskId) {
		super(actorRef, paths, taskId);
	}

}
