package com.fusion.messages;

import java.util.List;

import akka.actor.ActorRef;

public class ReadPartition extends WorkMessage{
	
	private final String partitionPath;
	
	public ReadPartition(final ActorRef workerRef, final String path) {
		super(workerRef);
		this.partitionPath = path;
	}

	public String getPartitionPath() {
		return partitionPath;
	}

}
