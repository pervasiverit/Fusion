package com.fusion.vertex;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import com.fusion.builder.BuilderException;
import com.fusion.builder.DataflowBuilder;
import com.fusion.edges.Edge;
import com.fusion.edges.EdgeList;
import com.fusion.io.OutputContext;
import com.fusion.utils.ConnectorType;
import com.fusion.utils.SerializableFunction;
import com.fusion.utils.SerializablePredicate;

@SuppressWarnings("rawtypes")
public abstract class AbstractVertex<T> implements Serializable {
	
	public static enum VertexType {
		POINT_WISE, SHUFFLE
	}
	
	protected VertexType type = VertexType.POINT_WISE;
	protected String vertexId;
	protected transient String name;
	protected EdgeList inputEdges;
	protected EdgeList outputEdges;
	private int nbrOfParents;
	private List<AbstractVertex> parentVertex = new ArrayList<>();
	
	public AbstractVertex() {
		
		this.inputEdges = new EdgeList();
		this.outputEdges = new EdgeList();
		vertexId = "Abstract Vertex " + this.getClass().toString()+ " "+this.hashCode();
	}
	
	public VertexType getVertexType(){
		return type;
	}
	public String getVertexId() {
		return vertexId;
	}

	public EdgeList getInput() {
		return inputEdges;
	}

	public EdgeList getOutput() {
		return outputEdges;
	}

	public abstract void execute(T Line, OutputContext collector) throws IOException;

	public void connectOutput(final int remotePort, AbstractVertex outputVertex, final int localPort,
			ConnectorType type) {
		Edge edge = new Edge(outputVertex, remotePort, type);
		outputEdges.setEdge(localPort, edge);
		edge = new Edge(this, localPort, type);
		outputVertex.getInput().setEdge(remotePort, edge);
	}

	@Override
	public String toString() {
		return vertexId + " " + this.getClass().getName();
	}
	
	public void close(OutputContext collector){
		
	}
	
	public void start(OutputContext collector) throws Exception{
		
	}
	
	public void addParent(AbstractVertex vertex){
		parentVertex.add(vertex);
	}
	
	
	public List<AbstractVertex> getParentRdd(){
		return parentVertex;
	}
	
	// Dependency on the commons-lang library.
	public AbstractVertex<T> makeClone()  {
		byte [] b = SerializationUtils.serialize(this);
		AbstractVertex<T> vertex = SerializationUtils.deserialize(b);
		return vertex;
	}

	public int getNbrOfPartitions() {
		return nbrOfParents;
	}

	public void setNbrOfPartitions(int nbrOfParents) {
		this.nbrOfParents = nbrOfParents;
	}
	
	
	public <U extends Comparable> Map<T,U> map(SerializableFunction<T, U> map, int p) {
		Map<T,U> map1 = new Map<>(map);
		map1.addParent(this);
		map1.type = VertexType.POINT_WISE;
		map1.setNbrOfPartitions(p);
		return map1;
	}

	public <X extends Comparable> Filter<X> filter(SerializablePredicate<X> filter, int p) {
		Filter<X> filter1= new Filter<>(filter);
		filter1.setNbrOfPartitions(p);
		filter1.addParent(this);
		filter1.type = VertexType.POINT_WISE;
		return filter1;
	}

	public VertexList sumInt() {
		SumInteger integer = new SumInteger();
		VertexList v = setInfo(integer);
		return v;
	}
	
	public VertexList maxInt() {
		MaxInteger integer = new MaxInteger();
		VertexList v = setInfo(integer);
		return v;
	}
	
	public VertexList minInt() {
		MinInteger integer = new MinInteger();
		VertexList v = setInfo(integer);
		return v;
	}
	
	public VertexList setInfo(AbstractVertex integer){
		integer.addParent(this);
		integer.type = VertexType.SHUFFLE;
		integer.setNbrOfPartitions(1);
		VertexList v = buildRoot(integer);
		return v;
	}

	private VertexList buildRoot(AbstractVertex integer) {
		DataflowBuilder builder = new DataflowBuilder();
		VertexList v1 = builder.createVertexSet(integer, integer.getNbrOfPartitions());
		VertexList v = null;
		try {
			v= builder.build(integer, v1);
		} catch (BuilderException | IOException e) {
			e.printStackTrace();
		}
		return v;
	}	
}
