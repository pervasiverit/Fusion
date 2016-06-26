package com.fusion.actors;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.fusion.messages.WorkRequest;
import com.fusion.scheduler.PointWiseStage;

import akka.actor.UntypedActor;

public class WorkerActor extends UntypedActor{
	private boolean isRunning;
	private ScheduledFuture scheduledFuture;
	
	public WorkerActor(){
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MethodUtils.invokeMethod(this, "handle",message);
	}
	
	public void handle(WorkRequest request){
		getSender().tell(request, getSelf());
	}
	
	public void handle(PointWiseStage stg){
		System.out.println(" Point wise stage received ...");
		try {
			stg.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
