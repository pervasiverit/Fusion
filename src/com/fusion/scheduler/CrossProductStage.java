package com.fusion.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.fusion.elements.BigIntegerElement;
import com.fusion.elements.Element;
import com.fusion.io.Collector;
import com.fusion.io.IntermediateRecord;
import com.fusion.io.OutputContext;
import com.fusion.io.OutputFormat;
import com.fusion.utils.ElementList;
import com.fusion.vertex.AbstractVertex;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CrossProductStage extends Stage {
	
	private static final long serialVersionUID = 7387991910464657727L;
	final private String tempPath = "tmp" + File.separator + getJobId() + File.separator + getTaskId();
	private final Class<? extends OutputFormat> outputFormat;
	private final File file;

	private List<Comparable> elementList;

	public CrossProductStage(final Class<? extends OutputFormat> outputFormat, final String jobId, final File outFile) {
		super(jobId);
		this.outputFormat = outputFormat;
		this.file = outFile;
	}

	@Override
	public void run() throws IOException {
		
		OutputContext collector = new OutputContext<>(tempPath);
		AbstractVertex abv = queue.poll();

		for(Object element : elementList){
			abv.execute(element, collector);
		}
		abv.close(collector);
		final int size = queue.size();
		FileUtils.forceMkdir(file);
		
		AbstractVertex reduceSide = null;
		for (int i = 0; i < size; ++i) {
			reduceSide = queue.poll();
			List<Object> buffer = collector.getBuffer();
			collector.clearBuffer();
			for(Object element: buffer){
				reduceSide.execute(element, collector);
			}
		}
		for(Object element : collector){
			
		}
	}
	
	
	public void setElementList(List<Comparable> e) {
		this.elementList = e;
	}
}
