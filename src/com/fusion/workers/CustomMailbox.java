package com.fusion.workers;

import com.fusion.messages.WorkToBeDone;
import com.fusion.workers.HeartBeatActor.HBMessage;
import com.fusion.workers.WorkerActor.WorkerState;
import com.typesafe.config.Config;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Terminated;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedStablePriorityMailbox;

public class CustomMailbox {
	
	static class ManagerMailbox extends UnboundedStablePriorityMailbox {

		public ManagerMailbox(ActorSystem.Settings settings, Config config) {
			super(new PriorityGenerator() {
				@Override
				public int gen(Object message) {
					if (message instanceof Terminated)
						return 0;
					else if(message instanceof WorkerState)
						return 1;
					else if (message instanceof WorkToBeDone)
						return 2;
					else if (message instanceof HBMessage)
						return 3;
					else if (message.equals(PoisonPill.getInstance()))
						return 5;
					else
						return 4;
				}
			});
		}
		
	}
	
	static class WorkerMailbox extends UnboundedStablePriorityMailbox {
		
		public WorkerMailbox(ActorSystem.Settings settings, Config config) {
			super(new PriorityGenerator() {
				@Override
				public int gen(Object message) {
					if (message.equals("highpriority"))
						return 0;
					else if (message.equals("lowpriority"))
						return 2;
					else if (message.equals(PoisonPill.getInstance()))
						return 3;
					else
						return 1;
				}
			});
		}
		
	}
	
}
