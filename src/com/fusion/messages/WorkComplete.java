package com.fusion.messages;

import java.util.Map;

import akka.actor.ActorRef;

public class WorkComplete extends WorkMessage{
	final Map<Integer, String> paths;
	final String taskId;
	
	public WorkComplete(final ActorRef actorRef, final Map<Integer, String> paths, final String taskId){
		super(actorRef);
		this.paths = paths;
		this.taskId = taskId;
	}
	
	public Map<Integer, String> getPaths(){
		return paths;
	}
	
	public String getTaskId(){
		return taskId;
	}
}
