package com.fusion.workers;

import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * Worker Main Program - Starts the daemon actors and heart beat actor
 * 
 * @author KanthKumar
 *
 */
public class Worker {
	public final int numOfCores;
	
	/**
	 * Default Constructor - Checks the available cores in current machine
	 * and initializes the same 
	 * 
	 */
	public Worker() {
		this.numOfCores = Runtime.getRuntime().availableProcessors();
	}
	
	/**
	 * Main Program 
	 * 
	 * @param args	Commans line arguments
	 */
	public static void main(String[] args) {
		Worker worker = new Worker();
		Config config = ConfigFactory.load("worker");
		ActorSystem system = ActorSystem.create("WorkerSystem", config);
		
		/*
		 * Starts the daemon actors, One for each available core
		 */
		List<ActorRef> daemons = new ArrayList<>();
		for(int i=0; i<worker.numOfCores; i++){
			daemons.add(system.actorOf(Props
					.create(DaemonActor.class), "DaemonActor"+i));
		}
		
		system.actorOf(Props.
				create(HeartBeatActor.class, config, daemons), "HeartBeatActor");
		
	}
}
