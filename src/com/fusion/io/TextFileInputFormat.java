package com.fusion.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Text File Input Format.
 * 
 * @author sumanbharadwaj
 *
 */
public class TextFileInputFormat implements InputFormat<String> {

	private final File file;
	private BufferedReader stream;
	private FileReader fis;
	private long start;
	private long numOfRecords;
	private final long end;
	private boolean extraRead;

	public TextFileInputFormat(final File file, final Long start, final Long end) {
		if (file == null) {
			throw new NullPointerException("File information is < null >");
		}
		this.file = file;
		this.start = start;
		this.end = end;
		this.numOfRecords = start;
	}

	@Override
	public void open() throws IOException {
		if (fis == null) {
			fis = new FileReader(file);
			boolean skipFirstLine = false;
			stream = new BufferedReader(fis);
			if (start != 0) {
				skipFirstLine = true;
				--start;
				stream.skip(start);
			}
			if (skipFirstLine) {
				String str = stream.readLine();
				System.out.println(str);
				start += str.length() + 1;
			}
		}
	}

	@Override
	public String next() throws IOException {
		if (stream == null) {
			throw new IOException("Unable to Open the file");
		}
		String line = stream.readLine();
		if (line != null)
			numOfRecords = numOfRecords + line.length() + 1;
		if(numOfRecords <= end){
			return line != null? line : null;
		}else{
			if(!extraRead){
				extraRead = true;
				return line != null? line : null;
			}
			return null;
		}
		
	}

	@Override
	public void close() throws IOException {
		if (stream == null) {
			throw new IOException("Unable to close the file");
		}
		fis = null;
		stream = null;
	}

	@Override
	public String toString() {
		return "Total number of records is " + numOfRecords;
	}

	/**
	 * Unit Test. To test threading.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		File file = new File("CRT");
		class pt extends Thread {
			TextFileInputFormat text;

			public pt(long stageId) throws IOException {
				long length = file.length();
				
				long temp = (long) Math.ceil((length / 2));
				
				long offset = stageId * temp;
				long end = offset + temp;
				System.out.println(String.format("Length %d temp %d offset % d", length, temp, offset));
				this.text = new TextFileInputFormat(file, offset, end);
				text.open();
			}

			@Override
			public void run() {

				try {
					String str;
					while ((str = text.next()) != null) {
						System.out.println(this + " " + str);
					}

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

		pt p = new pt(0);
		pt p1 = new pt(1);
		p.start();
		p1.start();
	}

}
