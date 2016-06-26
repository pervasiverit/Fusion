package com.fusion.actors;

import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.actor.Props;

public class Server {
	public static void main(String[] args) {
		ActorSystem system = ActorSystem.create("ServerSystem", 
				ConfigFactory.load("ServerConfig"));
		system.actorOf(Props.create(ServerActor.class), "ServerActor");
	}
}
