package com.fusion.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;

public class TextFileOutputFormat implements OutputFormat {

	private OutputStream stream;
	private PrintWriter writer;
	private File file;

	public TextFileOutputFormat(File file) {
		if (file == null) {
			throw new NullPointerException("File is null");
		}
		this.file = new File(file.getAbsolutePath()+File.separator+"part");
	}

	@Override
	public void open() throws IOException {
		stream = new FileOutputStream(file.getAbsolutePath());
		writer = new PrintWriter(stream);
	}

	@Override
	public void write(Object line) {
		writer.println(line);
	}

	@Override
	public void close() {
		writer.flush();
		writer.close();
		file = null;
	}
	
	
	public static void main(String[] args) throws IOException {
		File file = new File("sumout");
		FileUtils.forceMkdir(file);
		TextFileOutputFormat tof = new TextFileOutputFormat(file);
		tof.open();
		for(int i=0 ; i< 10; i++){
			tof.write("hi");
		}
		tof.close();
	}

}
