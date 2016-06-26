package com.fusion.workers;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.Iterable;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.elements.Element;
import com.fusion.messages.ReadPartition;
import com.fusion.messages.ReduceWorkToBeDone;
import com.fusion.scheduler.CrossProductStage;
import com.fusion.utils.Constants;
import com.fusion.utils.ElementList;
import com.fusion.workers.WorkerActor.WorkerState;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.util.ByteString;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * Copy Partition Actor class - started by worker manager to read the
 * partition files specified in stage before worker executes the Cross-
 * ProductStage (Reduce phase)
 * 
 * @author KanthKumar
 *
 */
public class CopyPartitionActor extends UntypedActor {

	private final ActorRef workerActor;
	private final ActorSystem system = getContext().system();
	
	/**
	 * Constructor
	 * 
	 * @param ref worker actor
	 */
	public CopyPartitionActor(ActorRef ref) {
		this.workerActor = ref;
	}
	
	@Override
	public void onReceive(Object msg) throws Exception {
		getContext().parent().tell(WorkerState.BUSY, workerActor);
		MethodUtils.invokeExactMethod(this, Constants.HANDLER, msg);
	}
	
	/**
	 * Get the required partition files, process and set the element 
	 * list to the stage.
	 * 
	 * @param workToDo
	 * @throws Exception
	 */
	public void handle(ReduceWorkToBeDone workToDo) throws Exception {
		Map<ActorRef, List<String>> mappers = workToDo.getActorPathMapping();
		List<Future<Object>> futures = new ArrayList<>();
		Timeout timeout = new Timeout(Duration.create(10, "seconds"));
		mappers.forEach((mapper, partitionPaths)->partitionPaths.forEach(
				(partitionPath)->futures.add(Patterns.ask(mapper, 
						new ReadPartition(getSelf(), partitionPath), timeout))));
		
		Future<Iterable<Object>> future = Futures.sequence(futures, system.dispatcher());
		Iterable<Object> iterable = Await.result(future, timeout.duration());
		List<Comparable> elementsList = process(iterable);
		CrossProductStage stage = (CrossProductStage) workToDo.getStage();
		stage.setElementList(elementsList);
		
		workerActor.forward(workToDo, getContext());
	}

	/**
	 * Process all the partition files sent as ByteString from workers
	 * which executed PointWiseStage (Map)
	 * 
	 * @param iterable list of partition files
	 * @return Element List
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Comparable> process(Iterable<Object> iterable) throws Exception {
		Iterator<Object> it = iterable.iterator();
		
		List<Comparable> list = new ArrayList<>();
		Comparable ele;
		while(it.hasNext()) {
			ByteString file = (ByteString) it.next();
			ObjectInputStream in = new ObjectInputStream(new 
						ByteArrayInputStream(file.toArray()));
			while((ele = (Comparable)in.readObject()) != null) {
				list.add(ele);
			}
		}
		
		Collections.sort(list);
		return list;
	}
	
}
