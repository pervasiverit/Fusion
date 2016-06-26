package com.fusion.io;

import java.io.File;

import com.fusion.elements.Element;
import com.google.code.externalsorting.ExternalSort;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Collector<T extends Element> implements Iterable<IntermediateRecord<T>>{

	private final List<IntermediateRecord<T>> buffer;
	private final String tmpDir;

	public Collector(String path) {
		buffer = new LinkedList<>();
		this.tmpDir = path;
	}
	
	public void clearBuffer(){
		buffer.clear();
	}
	
	public void add(T element) throws IOException {
		buffer.add(new IntermediateRecord<T>(element));
	}

	public String finish() throws IOException {
		String path = this.tmpDir + File.separator + "records.sorted " + Thread.currentThread().getId();
		snapshot(path);
		return path;
	}
	
	public List<IntermediateRecord<T>> getBuffer(){
		return Collections.unmodifiableList(new LinkedList<>(buffer));
	}

	private void snapshot(String path) throws IOException {
		Collections.sort(buffer);
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
		for (IntermediateRecord<T> record : buffer) {
			out.writeObject(record.getElement());
		}
		out.writeObject(null);
		out.flush();
		out.close();
		buffer.clear();
	}



	@Override
	public Iterator<IntermediateRecord<T>> iterator() {
		return buffer.iterator();
	}

}
