package com.fusion.io;

import java.io.IOException;
import java.io.Serializable;

public interface InputFormat<T> extends Serializable{
	public void open() throws IOException;
	public T next() throws IOException;
	public void close() throws IOException;
}
