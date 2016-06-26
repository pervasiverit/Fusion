package com.fusion.workers;

import com.fusion.messages.ConnectionComplete;
import com.fusion.messages.WorkMessage;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

/**
 * Daemon Actor class, receives and forwards the messages to manager
 * Acts like proxy and entry point for job controller.
 * 
 * @author KanthKumar
 *
 */
public class DaemonActor extends UntypedActor{
	
	private ActorRef manager;

	/**
	 * Starts the worker manager
	 */
	@Override
	public void preStart() throws Exception {
		manager = getContext().actorOf(Props.
				create(WorkerManager.class), "WorkerManager");
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof WorkMessage) {
			manager.forward(msg, getContext());
		}
		else if(msg instanceof ConnectionComplete) {
			manager.forward(msg, getContext());
		}
		else 
			unhandled(msg);
	}

}
