package com.fusion.builder;

import java.io.Serializable;

public class BuilderException extends Exception implements Serializable{

	public BuilderException(){
		
	}
	
	public BuilderException(String message){
		super(message);
	}
	
	public BuilderException(String message, Throwable cause){
		super(message, cause);
	}
	
	public BuilderException(Throwable cause){
		super(cause);
	}
}
