package com.fusion.builder;

import java.io.IOException;

import com.fusion.elements.IntegerElement;
import com.fusion.io.OutputContext;
import com.fusion.utils.ConnectorType;
import com.fusion.utils.SerializablePredicate;
import com.fusion.vertex.AbstractVertex;
import com.fusion.vertex.SumInteger;
import com.fusion.vertex.VertexList;
import com.fusion.vertex.AbstractVertex.VertexType;
@SuppressWarnings("rawtypes")
public final class DataflowBuilder {

	
	public void mapPointWise(VertexList source, VertexList destination, ConnectorType type)
			throws BuilderException, IOException {

		final int nbrInputs = source.size();
		final int nbrOutputs = destination.size();

		if (nbrOutputs == 1) {
			AbstractVertex outputVertex = destination.get(0);
			final int inputs = outputVertex.getInput().size();
			outputVertex.getInput().addEdges(inputs + nbrInputs);

			for (int i = 0; i < nbrInputs; i++) {
				AbstractVertex inputVertex = source.get(i);
				final int outputs = inputVertex.getOutput().size();
				inputVertex.getOutput().addEdges(1);
				inputVertex.connectOutput(outputs, outputVertex, inputs + i, type);
			}

		} else if (nbrInputs == 1) {
			AbstractVertex inputVertex = source.get(0);
			final int outputs = inputVertex.getOutput().size();
			inputVertex.getOutput().addEdges(outputs + nbrOutputs);
			for (int i = 0; i < nbrOutputs; i++) {
				AbstractVertex outputVertex = destination.get(i);
				final int inputs = outputVertex.getInput().size();
				outputVertex.getInput().addEdges(inputs + 1);
				inputVertex.connectOutput(outputs + i, outputVertex, inputs, type);
			}

		} else if (nbrInputs == nbrOutputs) {
			for (int i = 0; i < nbrInputs; i++) {
				AbstractVertex inputVertex = source.get(i);
				AbstractVertex outputVertex = destination.get(i);
				final int outputs = inputVertex.getOutput().size();
				final int inputs = outputVertex.getInput().size();
				inputVertex.getOutput().addEdges(outputs + 1);
				outputVertex.getInput().addEdges(inputs + 1);
				inputVertex.connectOutput(outputs, outputVertex, inputs, type);
			}
		} else {
			throw new BuilderException("Incorrect Mapping of source and destination");
		}
	}

	public void crossProduct(VertexList source, VertexList destination, ConnectorType type) {
		final int nbrSrc = source.size();
		final int nbrDest = destination.size();

		for (int i = 0; i < nbrSrc; i++) {
			source.get(i).getOutput().setNumberOfEdges(nbrDest);
		}

		for (int i = 0; i < nbrDest; i++) {
			AbstractVertex outputVertex = destination.get(i);
			final int inputs = outputVertex.getInput().size();
			outputVertex.getInput().addEdges(nbrSrc);
			for (int j = 0; j < nbrSrc; j++) {
				AbstractVertex inp = source.get(j);
				inp.connectOutput(i, outputVertex, i, type);
			}
		}
	}

	public VertexList createVertexSet(AbstractVertex prototype, final int copies) {
		VertexList output = new VertexList();
		for (int i = 0; i < copies; i++) {
			AbstractVertex vertex = prototype.makeClone();
			output.add(vertex);
		}
		return output;
	}

	public VertexList createVertexSet(Class<? extends AbstractVertex> prototype, final int copies) {

		VertexList output = new VertexList();
		for (int i = 0; i < copies; i++) {
			AbstractVertex vertex = null;
			try {
				vertex = prototype.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			output.add(vertex);
		}
		return output;
	}

	public VertexList build(AbstractVertex vertex, VertexList v1) 
			throws BuilderException, IOException {
		if(vertex.getParentRdd().isEmpty()){
			return v1;
		}
		VertexList temp = null;
		for(Object dest: vertex.getParentRdd()){
			AbstractVertex v = (AbstractVertex)dest;
			VertexList v2 = createVertexSet(v, v.getNbrOfPartitions());
			if(vertex.getVertexType() == VertexType.POINT_WISE){
				mapPointWise(v2, v1, ConnectorType.FILE);
			}else{
				crossProduct(v2, v1, ConnectorType.FILE);
			}
			temp = build(v, v2);
		}
		return temp;
	}
}
