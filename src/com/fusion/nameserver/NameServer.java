package com.fusion.nameserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * Name Server Main Program - Connects to JobController specified
 * in nameserver.conf
 * 
 * @author KanthKumar
 *
 */
public class NameServer {
	
	/**
	 * Main Program
	 * 
	 * @param args	Command line arguments
	 */
	public static void main(String[] args) {
		Config config = ConfigFactory.load("nameserver");
		ActorSystem system = ActorSystem.create("NameServer", config);
		system.actorOf(Props.create(NameServerActor.class, config), 
				"NameServerActor");
	}
	
}
