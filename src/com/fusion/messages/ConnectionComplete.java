package com.fusion.messages;

import akka.remote.RemoteActorRef;

public class ConnectionComplete {
	private final RemoteActorRef nameserver;
	
	public ConnectionComplete(RemoteActorRef nameserver) {
		this.nameserver = nameserver;
	}
	
	public RemoteActorRef getNameServer(){
		return nameserver;
	}
}
