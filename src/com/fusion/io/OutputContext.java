package com.fusion.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OutputContext<T extends Comparable<T>> implements Iterable<T>{
	
	private final List<T> buffer;
	private final String tmpDir;
	
	public OutputContext(String tmpDir) {
		this.buffer= new ArrayList<>();
		this.tmpDir = tmpDir;
	}
	
	public void add(T element) throws IOException {
		buffer.add(element);
	}
	
	public void clearBuffer(){
		buffer.clear();
	}
	
	public String finish() throws IOException {
		String path = this.tmpDir + File.separator + "records.sorted " + Thread.currentThread().getId();
		snapshot(path);
		return path;
	}
	
	public List<T> getBuffer(){
		return Collections.unmodifiableList(new ArrayList<>(buffer));
	}

	private void snapshot(String path) throws IOException {
		Collections.sort(buffer);
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
		for (T record : buffer) {
			out.writeObject(record);
		}
		out.writeObject(null);
		out.flush();
		out.close();
		buffer.clear();
	}
	
	@Override
	public Iterator<T> iterator() {
		return buffer.iterator();
	}

}
