package com.fusion.vertex;

import java.io.File;
import java.io.IOException;

import com.fusion.elements.StringElement;
import com.fusion.io.Collector;
import com.fusion.io.InputFormat;
import com.fusion.io.OutputContext;
import com.fusion.io.TextFileInputFormat;

@SuppressWarnings({"rawtypes","unchecked"})
public class InputVertex extends AbstractVertex<String>{

	private static final long serialVersionUID = 1L;
	private TextFileInputFormat inputFormat;
	private File file;
	
	@Override
	public String toString(){
		return "IO Vertex" + " " + this.hashCode();
	}

	public InputVertex(int partitions, File file){
		this.file = file;
		setNbrOfPartitions(partitions);
	}
	
	@Override
	public void start(OutputContext collector) throws Exception {
		long length = file.length();
		long temp = (long) Math.ceil((length / getNbrOfPartitions()));
		
		//TODO: Need to change offset to handle multiple instances. Introduce rank
		long offset = 0;
		long end = offset + temp;
		inputFormat = new TextFileInputFormat(file, offset, end);
		inputFormat.open();
	}
	
	@Override
	public void execute(final String Line, final OutputContext collector) throws IOException {
		
		String str= "";
		while((str = (String) inputFormat.next())!=null){
			System.out.println(str);
			collector.add(str);
		}
	}
	
	@Override
	public void close(OutputContext collector) {
		try {
			inputFormat.close();
		} catch (IOException e) {
		}
	}
	
	
}
