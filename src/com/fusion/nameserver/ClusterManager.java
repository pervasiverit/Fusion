package com.fusion.nameserver;

import java.util.HashMap;
import java.util.Map;

import com.fusion.messages.NodeTerminated;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;

public class ClusterManager extends UntypedActor{
	
	private final Map<ActorRef, ActorRef> nodes;
	
	public ClusterManager() {
		this.nodes = new HashMap<>();
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		if(msg instanceof ActorRef){
			ActorRef node = (ActorRef) msg;
			nodes.computeIfAbsent(node, (n)->getContext().watch(n));
		}
		else if(msg instanceof Terminated){
			ActorRef terminatedNode = ((Terminated) msg).getActor();
			nodes.remove(terminatedNode);
			getContext().parent().tell(new NodeTerminated(terminatedNode), 
					getSelf());
		}
		else {
			unhandled(msg);
		}
	}

}
