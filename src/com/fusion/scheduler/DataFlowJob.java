package com.fusion.scheduler;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

import com.fusion.builder.DataflowBuilder;
import com.fusion.edges.Edge;
import com.fusion.io.InputFormat;
import com.fusion.io.OutputFormat;
import com.fusion.io.TextFileInputFormat;
import com.fusion.io.TextFileOutputFormat;
import com.fusion.partitioner.Partitioner;
import com.fusion.vertex.AbstractVertex;
import com.fusion.vertex.InputVertex;
import com.fusion.vertex.VertexList;
import com.fusion.vertex.AbstractVertex.VertexType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;

/**
 * Define the job class. If the job
 * 
 * @author sumanbharadwaj
 *
 */
@SuppressWarnings("rawtypes")
public class DataFlowJob {

	final StageList stageList;
	private Class<? extends InputFormat> inputFormat;
	private File file;
	private File outFile;
	private Class<? extends OutputFormat> outputFormat;
	protected final String jobId = UUID.randomUUID().toString();
	private Class<? extends Partitioner> partitioner;
	
	public DataFlowJob() {
		stageList = new StageList();
	}

	public String getJobId() {
		return jobId;
	}
	
	private Class<? extends InputFormat> getInputFormatClass(){
		return inputFormat;
	}
	
	private Class<? extends OutputFormat> getOutputFormatClass(){
		return outputFormat;
	}

	/**
	 * Set Input Format class name
	 * 
	 * @return InputFormat
	 */
	public void setInputFormat(Class<? extends InputFormat> inf) {
		this.inputFormat = inf;
	}
	
	/**
	 * TODO: If time permits change it to a CompletableFuture.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run() throws IOException {
		ActorSystem actorSystem = ActorSystem.create("localworker", 
				ConfigFactory.load("localworker"));
		Config conf = ConfigFactory.load("localworker");
		ActorSelection actor = actorSystem.actorSelection(conf.getString("akka.actor.job-manager"));
		actor.tell(stageList, ActorRef.noSender());
		
		System.out.println("Finished..");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		// system.shutdown();
	}

	Queue<VertexList> queue = new ArrayDeque<>();
	private boolean visited = false;
	/**
	 * Set up stages
	 * 
	 * @param io 	IO Vertex list
	 */
	public void setRoot(VertexList io) {
		queue.add(io);
		while (!queue.isEmpty()) {
			visited = false;
			int stageID = 0; 
			int stageTotal = io.size();
			for (AbstractVertex vertex : queue.poll()) {
				if (vertex.getVertexType() == VertexType.POINT_WISE){
					PointWiseStage stg = new PointWiseStage(getInputFormatClass(), 
							getPartitionerClass(), jobId);
					stg.setStageTotal(stageTotal);
					stg.setStageId(stageID++);
					stageList.add(getStage(vertex, new VertexList(), stg));
				}
				else {
					Stage stg  = new CrossProductStage(getOutputFormatClass(), jobId, outFile);
					stageList.add(getStage(vertex, new VertexList(), stg));
				}
			}
		}
	}

	/**
	 * BAD:
	 * 
	 * Stage is effectively final. Do not change it
	 * 
	 * @param rootVertex
	 * @param vList
	 * @param stage
	 * @return
	 */
	private Stage getStage(final AbstractVertex rootVertex, final VertexList vList, final Stage stage) {
		if(rootVertex.getOutput().size() == 0){
			stage.addVertexList(rootVertex);
			return stage;
		}
		if (rootVertex.getOutput().get(0).getRemoteVertex().getVertexType() == VertexType.SHUFFLE)
			return manageShuffle(rootVertex, vList, stage);
		vList.add(rootVertex);

		for (Edge e : rootVertex.getOutput()) {
			getStage(e.getRemoteVertex(), vList, stage);
		}
		return stage;
	}

	

	private Stage manageShuffle(AbstractVertex rootVertex, VertexList vList, Stage stage) {
		vList.add(rootVertex);
		stage.setPartitionCount(rootVertex.getOutput().size());
		for (AbstractVertex stageVertex : vList) {
			stage.addVertexList(stageVertex);
		}
		if (!visited) {
			createVertexListAndToQueue(rootVertex);
		}
		return stage;
	}

	private void createVertexListAndToQueue(AbstractVertex rootVertex) {
		VertexList v = new VertexList();
		rootVertex.getOutput().stream().forEach(e -> v.add(e.getRemoteVertex()));
		queue.add(v);
		visited = true;
	}

	/**
	 * Set Input Path of the file in the Job
	 * 
	 * @param filePath
	 */
	public void setInputPath(String filePath) {
		this.file = new File(filePath);

	}

	public void setOutputPath(String outPath) {
		this.outFile = new File(outPath);
	}

	/**
	 * configure the user specified output format.
	 * 
	 * @param of
	 */
	public void setOutputFormat(Class<? extends OutputFormat> of) {
		this.outputFormat = of;
	}

	private Class<? extends Partitioner> getPartitionerClass() {
		return partitioner;
	}

	public void setPartitioner(Class<? extends Partitioner> partitioner) {
		this.partitioner = partitioner;
	}

	public InputVertex readTextFile(String filePath) {
		this.inputFormat = TextFileInputFormat.class;
		this.file = new File(filePath);
		InputVertex input = new InputVertex(1, this.file);
		return input;
	}
	
	public void writeTextFile(String outPath) {
		this.outputFormat = TextFileOutputFormat.class;
		this.outFile = new File(outPath);
	}
}
