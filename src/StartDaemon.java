import com.fusion.actors.JobControllertemp;
import com.fusion.actors.WorkerActor;
import com.fusion.actors.WorkerExec;
import com.fusion.messages.RegisterWorker;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Function;

public class StartDaemon {
	final static int jobPort = 5919;
	final static int workerPort = 5929;

	public static void main(String[] args) throws Exception {
		start();
		Thread.sleep(1000);
		
	}

	private static void start() {
		final Config conf = ConfigFactory.load("JobManager");
		final ActorSystem system = ActorSystem.create("JobSystem", conf);
		ActorRef actorJ = system.actorOf(Props.create(JobControllertemp.class), "JobActor");
	}

}
