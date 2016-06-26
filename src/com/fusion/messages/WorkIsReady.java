package com.fusion.messages;

import akka.actor.ActorRef;

public class WorkIsReady extends WorkMessage{

	public WorkIsReady(ActorRef workerRef) {
		super(workerRef);
	}

}
