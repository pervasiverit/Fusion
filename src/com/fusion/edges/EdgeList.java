package com.fusion.edges;

import java.util.ArrayList;

public class EdgeList extends ArrayList<Edge>{

	public void addEdges(final int newInputs) {
		assert newInputs >= size();
		int newCount = newInputs - size();
		for(int i=0; i< newInputs; i++){
			add(new Edge());
		}
	}

	public void setNumberOfEdges(int nbrDest) {
		for(int i=0; i < nbrDest; i++){
			add(new Edge());
		}
	}

	public void setEdge(int port, Edge edge) {
		if(port < size()){
			set(port,edge);
		}else{
			add(edge);
		}
		
	}
	
}
