package com.fusion.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.ReduceWorkToBeDone;
import com.fusion.messages.WorkComplete;
import com.fusion.messages.WorkMessage;
import com.fusion.messages.WorkToBeDone;
import com.fusion.scheduler.CrossProductStage;
import com.fusion.scheduler.PointWiseStage;
import com.fusion.scheduler.Stage;

import akka.actor.ActorRef;

public class WorkStatus {

	// pending work
	private ConcurrentLinkedDeque<Stage> pendingWork = new ConcurrentLinkedDeque<>();

	// Job in progress
	private Map<ActorRef, Stage> workInProgress = new HashMap<>();

	// accepted work ids.
	private HashSet<String> acceptedWorkIds = new HashSet<>();

	// completed work ids.
	private HashSet<String> completedWorkIds = new HashSet<>();

	public boolean hasWork() {
		return !pendingWork.isEmpty();
	}

	public Stage next() {
		return pendingWork.getFirst();
	}

	public WorkStatus() {

	}
	
	public int getMapperCount(){
		return (int)workInProgress.entrySet().stream()
							   .filter(e -> e.getValue() instanceof PointWiseStage)
							   .count();
	}
	
	public int getWorkInProgressCount(){
		return workInProgress.size();
	}

	public WorkStatus getInstance(WorkStatus curr, Object message) throws Exception {
		return (WorkStatus) ConstructorUtils.invokeConstructor(this.getClass(), curr, message);
	}

	public WorkStatus(WorkStatus currentWorkState, WorkToBeDone workToBeDone) {
		ConcurrentLinkedDeque<Stage> tmp_pendingWork = new ConcurrentLinkedDeque<>(currentWorkState.pendingWork);
		Map<ActorRef, Stage> tmp_workInProgress = new HashMap<>(currentWorkState.workInProgress);
		Stage stage = tmp_pendingWork.removeFirst();
		if (!stage.getTaskId().equals(workToBeDone.getStage().getTaskId())) {
			throw new IllegalArgumentException("Error expected  ");
		}
		tmp_workInProgress.put(workToBeDone.getActorRef(), stage);
		workInProgress = tmp_workInProgress;
		acceptedWorkIds = new HashSet<String>(currentWorkState.acceptedWorkIds);
		completedWorkIds = new HashSet<String>(currentWorkState.completedWorkIds);
		pendingWork = tmp_pendingWork;
	}
	
	
	public WorkStatus(WorkStatus currentWorkState, ReduceWorkToBeDone workToBeDone) {
		ConcurrentLinkedDeque<Stage> tmp_pendingWork = new ConcurrentLinkedDeque<>(currentWorkState.pendingWork);
		Map<ActorRef, Stage> tmp_workInProgress = new HashMap<>(currentWorkState.workInProgress);
		Stage stage = tmp_pendingWork.removeFirst();
		if (!stage.getTaskId().equals(workToBeDone.getStage().getTaskId())) {
			throw new IllegalArgumentException("Error expected  ");
		}
		tmp_workInProgress.put(workToBeDone.getActorRef(), stage);
		workInProgress = tmp_workInProgress;
		acceptedWorkIds = new HashSet<String>(currentWorkState.acceptedWorkIds);
		completedWorkIds = new HashSet<String>(currentWorkState.completedWorkIds);
		pendingWork = tmp_pendingWork;
	}

	public WorkStatus(WorkStatus curr, WorkComplete message) {
		Map<ActorRef, Stage> tmp_workInProgress = new HashMap<>(curr.workInProgress);
		HashSet<String> tmp_doneWorkIds = new HashSet<String>(curr.completedWorkIds);
		tmp_workInProgress.remove(message.getActorRef());
		tmp_doneWorkIds.add(message.getTaskId());
		workInProgress = tmp_workInProgress;
		acceptedWorkIds = new HashSet<String>(curr.acceptedWorkIds);
		completedWorkIds = tmp_doneWorkIds;
		pendingWork = new ConcurrentLinkedDeque<>(curr.pendingWork);
		System.out.println(workInProgress);
	}

	public WorkStatus(WorkStatus curr, Stage workAccepted) {
		ConcurrentLinkedDeque<Stage> tmp_pendingWork = new ConcurrentLinkedDeque<>(curr.pendingWork);
		HashSet<String> tmp_acceptedWorkIds = new HashSet<String>(curr.acceptedWorkIds);
		tmp_pendingWork.addLast(workAccepted);
		tmp_acceptedWorkIds.add(workAccepted.getTaskId());
		workInProgress = new HashMap<>(curr.workInProgress);
		acceptedWorkIds = tmp_acceptedWorkIds;
		completedWorkIds = new HashSet<String>(curr.completedWorkIds);
		pendingWork = tmp_pendingWork;
	}
	
	
//	public static void main(String[] args) throws Exception {
//		WorkStatus status = new WorkStatus();
//		PointWiseStage stage = new PointWiseStage(null, null, "as", null);
//		PointWiseStage stage1 = new PointWiseStage(null, null, "a1s", null);
//		PointWiseStage stage2 = new PointWiseStage(null, null, "as2", null);
//		PointWiseStage stage3 = new PointWiseStage(null, null, "as3", null);
//		CrossProductStage stage4 = new CrossProductStage(null, "a", null);
//		status = status.getInstance(status, stage);
//		status = status.getInstance(status, stage1);
//		status = status.getInstance(status, stage2);
//		status = status.getInstance(status, stage3);
//		status = status.getInstance(status, stage4);
//		status = status.getInstance(status, new WorkToBeDone(ActorRef.noSender(), stage, "a"));
//		status = status.getInstance(status, new WorkToBeDone(ActorRef.noSender(), stage1, "a"));
//		status = status.getInstance(status, new WorkToBeDone(ActorRef.noSender(), stage2, "a"));
//		status = status.getInstance(status, new WorkToBeDone(ActorRef.noSender(), stage3, "a"));
//		status = status.getInstance(status, new WorkComplete(ActorRef.noSender(), null, "a"));
//		status = status.getInstance(status, new WorkComplete(ActorRef.noSender(), null, "a"));
//		status = status.getInstance(status, new WorkComplete(ActorRef.noSender(), null, "a"));
//		status = status.getInstance(status, new WorkComplete(ActorRef.noSender(), null, "a"));
//		System.out.println(status.getMapperCount() == 1);
//		
//		
//	}

}
