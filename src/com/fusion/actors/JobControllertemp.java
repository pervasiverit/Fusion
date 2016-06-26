package com.fusion.actors;

import static com.fusion.utils.Constants.HANDLER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.MapWorkComplete;
import com.fusion.messages.ReduceWorkComplete;
import com.fusion.messages.ReduceWorkToBeDone;
import com.fusion.messages.RegisterWorker;
import com.fusion.messages.WorkIsReady;
import com.fusion.messages.WorkRequest;
import com.fusion.messages.WorkToBeDone;
import com.fusion.scheduler.CrossProductStage;
import com.fusion.scheduler.PointWiseStage;
import com.fusion.scheduler.Stage;
import com.fusion.utils.IntermediatePathList;
import com.fusion.utils.PointWiseMap;
import com.fusion.utils.WorkStatus;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class JobControllertemp extends UntypedActor {

	// Store ActorRef and intermediate paths pointwisetasks
	final PointWiseMap completedPointWiseTasks = new PointWiseMap();

	// worker state and actor ref. Represents actors and its state.
	final HashMap<ActorRef, WorkerState> workers = new HashMap<>();

	// work status
	private WorkStatus workStatus = new WorkStatus();

	/**
	 * Worker sending a register worker message. Outcome: Put the worker in the
	 * list. And assign a task immediately if available.
	 * 
	 * @param message
	 */
	public void handle(RegisterWorker message) {
		workers.put(message.getActorRef(), new WorkerState(message.getActorRef(), Idle.instance));
		System.out.println("Register Worker Message..");
		if (workStatus.hasWork()) {
			message.getActorRef().tell(new WorkIsReady(getSelf()), getSelf());
		}
	}

	/**
	 * Handle the work request from either workers or nameserver
	 * 
	 * @param work
	 */
	@SuppressWarnings("unchecked")
	public void handle(WorkRequest work) throws Exception {
		if (workStatus.hasWork()) {
			System.out.println("Received a work Request Message..");
			final ActorRef ref = work.getActorRef();
			Object obj = MethodUtils.invokeMethod(this, "getWorkToBeDone", workStatus.next(),ref);
			if(obj instanceof WorkToBeDone){
				WorkToBeDone toBeDone = (WorkToBeDone) obj;
				String taskId = toBeDone.getStage().getTaskId();
				sendMessage(ref, toBeDone, taskId);
				
			}else{
				Optional<ReduceWorkToBeDone> castedObj = (Optional<ReduceWorkToBeDone>) obj;
				if(castedObj.isPresent()){
					ReduceWorkToBeDone toBeDone = castedObj.get();
					String taskId = toBeDone.getStage().getTaskId();
					sendMessage(ref, toBeDone, taskId);
				}
				
			}
			
		}
	}
	
	private void sendMessage(ActorRef ref, Object toBeDone, String taskId) throws Exception{
		workStatus = workStatus.getInstance(workStatus, toBeDone);
		workers.put(ref, new WorkerState(getSender(), new Busy(taskId)));
		System.out.println(ref + " Sending a work to be done message..");
		ref.tell(toBeDone, getSelf());
	}

	/**
	 * Return an instance of the work to be done. put the stage inside the work
	 * to be done message and send it to a idle worker.
	 * 
	 * @param stage
	 * @param workRequest
	 * @param ref
	 * @return
	 */
	public WorkToBeDone getWorkToBeDone(PointWiseStage stage, ActorRef ref) {
		return new WorkToBeDone(ref, stage, "");
	}

	public Optional<ReduceWorkToBeDone> getWorkToBeDone(CrossProductStage stage, ActorRef ref) {
		if(workStatus.getMapperCount() != 0){
			return Optional.empty();
		}
		Optional<Map<ActorRef, List<String>>> map = completedPointWiseTasks.getMapping();
		if(map.isPresent()){
			return Optional.of(new ReduceWorkToBeDone(ref, stage, map.get()));
		}
		// Shouldn't hit this scenario in the current setup.
		completedPointWiseTasks.cleanUp(completedPointWiseTasks);
		return Optional.empty();
	}

	/**
	 * Handler for the map work completed message. Worker sends a path and its
	 * ActorRef. Store it in a hashmap
	 * 
	 * @param work
	 */
	public void handle(MapWorkComplete work) {
		System.out.println("Map Work Complete...");
		final ActorRef ref = work.getActorRef();
		System.out.println(ref);
		final Map<Integer, String> path = work.getPaths();
		System.out.println(path);
		workers.put(ref, new WorkerState(ref, Idle.instance));
		completedPointWiseTasks.addCompleted(ref, path);
		System.out.println(completedPointWiseTasks);
		try {
			workStatus = workStatus.getInstance(workStatus, work);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void handle(ReduceWorkComplete reduceWorkComplete){
		System.out.println("Reduce Work Complete...");
	}

	/**
	 * Add a new pointwise stage to the queue. And notify the works which are
	 * idle.
	 * 
	 * @param stage
	 */
	public void handle(PointWiseStage stage) {
		System.out.println("Adding Point wise stage..");
		try {
			workStatus = workStatus.getInstance(workStatus, stage);
		} catch (Exception e) {
			e.printStackTrace();
		}
		notifyWorkers();
	}

	/**
	 * notify all workers which are
	 */
	private void notifyWorkers() {
		workers.values().stream().filter(e -> e.status.isIdle())
				.forEach(e -> e.ref.tell(new WorkIsReady(getSelf()), getSender()));
	}

	public void handle(CrossProductStage stage) {
		System.out.println("Adding CrossProduct stage..");
		try {
			workStatus = workStatus.getInstance(workStatus, stage);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// notifyWorkers();
	}

	public void handle(String message) {
		System.out.println(message);
	}

	/**
	 * FOR THE LOVE OF GOD; CHECKED EXCEPTIONS LEAVE ME ALONE !!!!!
	 * 
	 * @param List<Stage>
	 *            stage
	 */
	public void handle(List<Stage> stage) {
		stage.stream().forEach(param -> {
			try {
				MethodUtils.invokeMethod(this, HANDLER, param);
			} catch (Exception e) {

			}
		});
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MethodUtils.invokeMethod(this, HANDLER, message);
	}

	private static abstract class WorkerStatus {
		protected abstract boolean isIdle();

		private int workerCount;

		private boolean isBusy() {
			return workerCount == 0 && !isIdle();
		};

		protected abstract String getWorkId();
	}

	private static final class Idle extends WorkerStatus {
		private static final Idle instance = new Idle();

		public static Idle getInstance() {
			return instance;
		}

		@Override
		protected boolean isIdle() {
			return true;
		}

		@Override
		protected String getWorkId() {
			throw new IllegalAccessError();
		}

		@Override
		public String toString() {
			return "Idle";
		}
	}

	private static final class Busy extends WorkerStatus {
		private final String workId;

		private Busy(String workId) {
			this.workId = workId;
		}

		@Override
		protected boolean isIdle() {
			return false;
		}

		@Override
		protected String getWorkId() {
			return workId;
		}

		@Override
		public String toString() {
			return "Busy{" + "work=" + workId;
		}
	}

	private static final class WorkerState {
		public final ActorRef ref;
		public final WorkerStatus status;

		private WorkerState(ActorRef ref, WorkerStatus status) {
			this.ref = ref;
			this.status = status;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || !getClass().equals(o.getClass()))
				return false;

			WorkerState that = (WorkerState) o;

			if (!ref.equals(that.ref))
				return false;
			if (!status.equals(that.status))
				return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = ref.hashCode();
			result = 31 * result + status.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "WorkerState{" + "ref=" + ref + ", status=" + status + '}';
		}
	}

}
