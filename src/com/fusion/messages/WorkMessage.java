package com.fusion.messages;

import java.io.Serializable;

import akka.actor.ActorRef;

public abstract class WorkMessage implements Serializable{

	final ActorRef workerRef;

	public WorkMessage(final ActorRef workerRef) {
		this.workerRef = workerRef;
	}

	public ActorRef getActorRef() {
		return workerRef;
	}

}
