package com.fusion.io;

import java.io.IOException;

public interface OutputFormat<T> {
	public void open() throws IOException;
	public void write(T line) throws IOException;
	public void close() throws IOException;
}
