package com.fusion.workers;

import java.util.List;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.ConnectionComplete;
import com.fusion.messages.Message;
import com.fusion.utils.Constants;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Status.Failure;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.remote.RemoteActorRef;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * Heart Beat Actor - Sends the heart beats to Name Server specifying 
 * Node is alive.
 * 
 * @author KanthKumar
 *
 */
public class HeartBeatActor extends UntypedActor{
	
	/**
	 * Heart Beat Message
	 * 
	 * @author KanthKumar
	 *
	 */
	static public class HBMessage implements Message {

		private static final long serialVersionUID = 1L;
		private static final HBMessage INSTANCE = new HBMessage();
		
		private HBMessage(){}
		
		public static HBMessage getInstance(){
			return INSTANCE;
		}
	}
	
	private final ActorSystem system = getContext().system();
	private final ActorSelection nameServer;
	private final List<ActorRef> daemons;
	
	public HeartBeatActor(Config config, List<ActorRef> daemons) {
		this.nameServer = system.actorSelection
				(config.getString("akka.actor.name-server"));
		this.daemons = daemons;
	}
	
	/**
	 * Re-tries connecting to name server
	 */
	public void tryConnectingToNameserver(){
		Future<ActorRef> future = nameServer.
				resolveOne(Duration.create(5, "seconds"));
		Patterns.pipe(future, system.dispatcher()).to(getSelf());
	}
	
	@Override
	public void preStart() throws Exception {
		tryConnectingToNameserver();
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
	}
	
	public void handle(RemoteActorRef ref){
		daemons.forEach((deamon)->deamon.
				tell(new ConnectionComplete(ref), getSelf()));
		
		system.scheduler().schedule(Duration.Zero(), 
				Duration.create(5, "seconds"), ref, HBMessage.getInstance(),
				system.dispatcher(), getSelf());
	}
	
	public void handle(Failure exception){
		tryConnectingToNameserver();
	}
}
