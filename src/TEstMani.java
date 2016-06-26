import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class TEstMani {
	public static void main(String[] args) {
		Config config = ConfigFactory.load("JobManager");
		ActorSystem system = ActorSystem.create("JobController", config);
		system.actorOf(Props.create(Actor.class), "JobActor");
		
		
	}

}


class Actor extends UntypedActor {
	
	@Override
	public void preStart() throws Exception {
		ActorSystem actorSystem = getContext().system();
		ActorSelection actor = actorSystem.actorSelection("akka.tcp://NameServer@129.21.12.236:2553/user/NameServerActor");
		actor.tell("helloooo", ActorRef.noSender());
	}

	@Override
	public void onReceive(Object msg) throws Exception {
		System.out.println(msg);
		
	}
	
}
