package com.fusion.scheduler;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.fusion.vertex.AbstractVertex;

/***
 * TODO: Very bad idea to supress "unchecked" exceptions.
 * Fix it when time ASAP.
 * @author sumanbharadwaj
 *
 */
public abstract class Stage implements Serializable {
	
	protected Queue<AbstractVertex> queue;
	protected String jobId;
	protected int stageIncr;
	protected int stageID;
	protected int partitionCount;
	protected Map<Integer, String> partitionFiles;
	
	protected String taskId = UUID.randomUUID().toString();
	
	
	public String getTaskId(){
		return taskId;
	}
	
	public String getJobId(){
		return jobId;
	}
	
	public Stage(final String jobId) {
		queue = new ConcurrentLinkedQueue<>();
		this.jobId = jobId;
	}

	public void addVertexList(final AbstractVertex v) {
		queue.add(v);
	}

	public abstract <T> void run() throws IOException;
	

	@Override
	public String toString() {
		return queue.toString();
	}

	public void setStageTotal(final int stageTotal) {
		stageIncr = stageTotal;
	}
	
	public void setStageId(final int stageId) {
		stageID = stageId;
	}
	
	public int getPartitionCount() {
		return partitionCount;
	}

	public void setPartitionCount(int count) {
		this.partitionCount = count;
	}

	public Map<Integer, String> getPartitionFiles() {
		return partitionFiles;
	}

	public void setPartitionFiles(Map<Integer, String> partitionFiles) {
		this.partitionFiles = partitionFiles;
	}
	
}
