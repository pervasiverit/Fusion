package com.fusion.nameserver;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Predicate;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.WorkMessage;
import com.fusion.utils.Constants;
import com.fusion.workers.HeartBeatActor.HBMessage;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.remote.RemoteActorRef;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * NameServer Actor class - receives work request and heart beat 
 * messages from workers.
 * Forwards heart beat to cluster manager and work request to job
 * controller
 * 
 * @author KanthKumar
 *
 */
public class NameServerActor extends UntypedActor{

	private final ActorSelection jobManager;
	private RemoteActorRef jobManagerRef;
	private final ActorRef clusterManager;
	private final ActorSystem system = getContext().system();
	private final Queue<? super WorkMessage> cache;
	private final Predicate<RemoteActorRef> connection = (ref) -> ref != null;
	
	/**
	 * Constructor
	 * 
	 * @param config name server config
	 */
	public NameServerActor(Config config) {
		this.cache = new LinkedList<>();
		this.jobManager = system.actorSelection
				(config.getString("akka.actor.job-manager"));
		this.clusterManager = getContext().actorOf(Props.
				create(ClusterManager.class), "ClusterManager");
	}
	
	public void tryConnectingToJobManager(){
		Future<ActorRef> future = jobManager.
				resolveOne(Duration.create(5, "seconds"));
		Patterns.pipe(future, system.dispatcher()).to(getSelf());
	}
	
	@Override
	public void preStart() throws Exception {
		//tryConnectingToJobManager();
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		//MethodUtils.invokeMethod(this, Constants.HANDLER, msg);
		if(msg instanceof HBMessage) {
			clusterManager.tell(getSender(), getSelf());
		}
		else {
			jobManager.tell(msg, getSelf());
		}
	}
	
	public void handle(HBMessage msg){
		clusterManager.tell(getSender(), getSelf());
	}
	
	public <T extends WorkMessage> void handle(T msg){
		if(connection.test(jobManagerRef))
			jobManagerRef.tell(msg, getSelf());
		else
			cache.add(msg);
	}
	
	public void handle(RemoteActorRef ref){
		this.jobManagerRef = ref;
		for(Object msg : cache){
			WorkMessage workMsg = (WorkMessage) msg;
			jobManagerRef.tell(workMsg, getSelf());
		}
	}
	
	public void handle(Failure exception){
		tryConnectingToJobManager();
	}
}
