package com.fusion.vertex;

import java.io.File;
import java.io.IOException;

import com.fusion.io.OutputContext;
import com.fusion.io.TextFileOutputFormat;

public class OutputVertex extends AbstractVertex<String>{

	private static final long serialVersionUID = 1L;
	private TextFileOutputFormat outputFormat;
	private File file;
	
	@Override
	public String toString(){
		return "IO Vertex" + " " + this.hashCode();
	}

	public OutputVertex(int partitions, File file){
		this.file = file;
		setNbrOfPartitions(partitions);
	}
	
	@Override
	public void start(OutputContext collector) throws Exception {
		outputFormat = new TextFileOutputFormat(file);
		outputFormat.open();
	}
	
	@Override
	public void execute(final String line, final OutputContext collector) throws IOException {
		outputFormat.write(line);
	}
	
	@Override
	public void close(OutputContext collector) {
		outputFormat.close();
	}
	
	
}

