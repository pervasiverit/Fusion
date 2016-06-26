package com.fusion.workers;

import java.util.Optional;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.ConnectionComplete;
import com.fusion.messages.ReadPartition;
import com.fusion.messages.ReduceWorkToBeDone;
import com.fusion.messages.RegisterWorker;
import com.fusion.messages.WorkIsReady;
import com.fusion.messages.WorkRequest;
import com.fusion.messages.WorkToBeDone;
import com.fusion.utils.Constants;
import com.fusion.workers.WorkerActor.WorkerState;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.japi.Function;
import akka.remote.RemoteActorRef;
import scala.concurrent.duration.Duration;

/**
 * Worker Manager class - starts the worker actor and supervises the 
 * same. applies supervisor strategy if worker actor gets terminated 
 * 
 * @author KanthKumar
 *
 */
public class WorkerManager extends UntypedActor{

	private RemoteActorRef nameServer;
	private ActorRef workerActor;
	private Cancellable notifer;
	private final ActorRef deamonActor = getContext().parent();
	private final ActorSystem system = getContext().system();
	
	/**
	 * Default constructor - starts and initializes worker actor
	 */
	public WorkerManager() {
		this.workerActor = createWorkerActor();
	}
	
	private ActorRef createWorkerActor(){
		ActorRef worker = getContext().actorOf(Props
				.create(WorkerActor.class, deamonActor)
				.withDispatcher("pool-dispatcher"));
	    getContext().watch(worker);
	    return worker;
	}
	
	/**
	 * Default strategy to restart the worker actor if terminates
	 */
	private SupervisorStrategy strategy = new OneForOneStrategy(10, 
			Duration.create(5, "seconds"), 
			new Function<Throwable, Directive>() {
	
				@Override
				public Directive apply(Throwable throwable) throws Exception {
					return SupervisorStrategy.restart();
				}
			});
	
	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		 MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
	}
	
	public void handle(ConnectionComplete complete) {
		nameServer = complete.getNameServer();
		RegisterWorker register = new RegisterWorker(deamonActor);
		nameServer.tell(register, getSelf());
	}
	
	public void handle(WorkIsReady workReady) {
		WorkRequest workReq = new WorkRequest(deamonActor);
		getSender().tell(workReq, getSelf());
	}

	public void handle(WorkToBeDone workToDo) {
		workerActor.forward(workToDo, getContext());
	}
	
	/**
	 * Forwards the message to copy partition actor that copies the required
	 * partition files from all remote machines specified within this message
	 * and then forwards the same message to worker to run the stage.
	 * 
	 * @param workToDo reduce work type
	 */
	public void handle(ReduceWorkToBeDone workToDo) {
		ActorRef copyActor = getContext().actorOf(Props
				.create(CopyPartitionActor.class, workerActor));
		copyActor.forward(workToDo, getContext());
	}
	
	public void handle(ReadPartition workToDo) {
		workerActor.forward(workToDo, getContext());
	}
	
	/**
	 * Handles the worker state (IDLE or BUSY), schedules the notifier if
	 * worker actor is idle otherwise cancels the notifier if scheduled 
	 * 
	 * @param state worker state
	 */
	public void handle(WorkerState state) {
		if(state == WorkerState.IDLE) {
			WorkRequest workReq = new WorkRequest(deamonActor);
			nameServer.tell(workReq, getSelf());
			notifer = system.scheduler().schedule(Duration.
					create(3, "seconds"), Duration.create(5, "seconds"), 
					nameServer, workReq, system.dispatcher(), getSelf());
		} else {
			if(notifer != null)
				notifer.cancel();
		}
	}
	
	public void handle(Terminated terminated) {
		workerActor = createWorkerActor();
	}
}
