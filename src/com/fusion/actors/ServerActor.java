package com.fusion.actors;

import akka.actor.PoisonPill;
import akka.actor.UntypedActor;

public class ServerActor extends UntypedActor{

	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println("Clent Says :"+msg);
		Thread.sleep(2000);
		/*if(msg instanceof Message){
			System.out.println("Clent Says :"+((Message)msg).getText());
			getSender().tell("I'm Done", getSelf());
			getSelf().tell(PoisonPill.getInstance(), getSelf());
		} else 
			unhandled(msg);*/
	}

}
