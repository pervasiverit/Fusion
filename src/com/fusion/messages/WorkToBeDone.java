package com.fusion.messages;

import com.fusion.scheduler.Stage;

import akka.actor.ActorRef;

public class WorkToBeDone extends WorkMessage{

	private final Stage stage;
	private final String path;
	
	public WorkToBeDone(final ActorRef workerRef,
			final Stage stage, final String path) {
		
		super(workerRef);
		this.stage = stage;
		this.path = path;
	}
	
	public Stage getStage(){
		return stage;
	}
	
	public String getPath(){
		return path;
	}
}
