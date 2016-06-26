package com.fusion.actors;

import static com.fusion.utils.Constants.HANDLER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.MapWorkComplete;
import com.fusion.messages.RegisterWorker;
import com.fusion.messages.WorkIsReady;
import com.fusion.messages.WorkRequest;
import com.fusion.messages.WorkToBeDone;
import com.fusion.scheduler.CrossProductStage;
import com.fusion.scheduler.PointWiseStage;
import com.fusion.scheduler.Stage;
import com.fusion.utils.PointWiseMap;
import com.fusion.utils.WorkStatus;

import akka.actor.ActorRef;
import akka.japi.Procedure;
import akka.persistence.UntypedPersistentActor;

public class JobController extends UntypedPersistentActor {

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
			System.out.println(getSelf());
			message.getActorRef().tell(new WorkIsReady(getSelf()), getSelf());
		}
	}

	/**
	 * Handle the work request from either workers or nameserver
	 * 
	 * @param work
	 */
	public void handle(WorkRequest work) throws Exception {
		if (workStatus.hasWork()) {
			System.out.println("Received a work Request Message..");
			final ActorRef ref = work.getActorRef();
			WorkToBeDone toBeDone = (WorkToBeDone) MethodUtils.invokeMethod(this, "getWorkToBeDone", workStatus.next(),
					work, ref);
			persist(toBeDone, new Procedure<WorkToBeDone>() {
				@Override
				public void apply(WorkToBeDone workToBeDone) throws Exception {
					workStatus = workStatus.getInstance(workStatus, workToBeDone);
					String taskId = toBeDone.getStage().getTaskId();
					workers.put(ref, new WorkerState(getSender(), new Busy(taskId)));
					System.out.println(ref + " Sending a work to be done message..");
					ref.tell(workToBeDone, getSelf());
				}
			});
		}
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
	public WorkToBeDone getWorkToBeDone(PointWiseStage stage, WorkRequest workRequest, ActorRef ref) {
		return new WorkToBeDone(ref, stage, "");
	}

	/**
	 * Handler for the map work completed message. Worker sends a path and its
	 * ActorRef. Store it in a hashmap
	 * 
	 * @param work
	 */
	public void handle(MapWorkComplete work) {
		final ActorRef ref = work.getActorRef();
		final Map<Integer,String> path = work.getPaths();
		workers.put(ref, new WorkerState(ref, Idle.instance));
		//completedPointWiseTasks.addCompleted(ref, path);
		persist(work, new Procedure<MapWorkComplete>() {
			@Override
			public void apply(MapWorkComplete work) throws Exception {
				workStatus = workStatus.getInstance(workStatus, work);
			}

		});
	}

	/**
	 * Add a new pointwise stage to the queue. And notify the works which are
	 * idle.
	 * 
	 * @param stage
	 */
	public void handle(PointWiseStage stage) {
		// final String jobID = stage.getJobId();
		System.out.println("Adding Point wise stage..");
		persist(stage, new Procedure<Stage>() {
			@Override
			public void apply(Stage stage) throws Exception {
				workStatus = workStatus.getInstance(workStatus, stage);
				notifyWorkers();
			}
		});
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
		persist(stage, new Procedure<Stage>() {
			@Override
			public void apply(Stage stage) throws Exception {
				workStatus = workStatus.getInstance(workStatus, stage);
				// notifyWorkers();
			}
		});
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
	public String persistenceId() {
		return "JobController";
	}

	@Override
	public void onReceiveCommand(Object message) throws Exception {
		MethodUtils.invokeMethod(this, HANDLER, message);
	}

	@Override
	public void onReceiveRecover(Object message) throws Exception {
		// workStatus = workStatus.getInstance(workStatus, message);
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
