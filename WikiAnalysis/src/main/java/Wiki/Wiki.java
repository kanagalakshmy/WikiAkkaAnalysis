package Wiki;

import java.io.IOException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import Wiki.Aggregator;

public class Wiki {

	public static void main(String[] args) throws IOException {
		ActorSystem system = ActorSystem.create("wikiActors");

		ActorRef aggregator = system.actorOf(Props.create(Aggregator.class), "aggregator");
		aggregator.tell(new Aggregator.Start(), ActorRef.noSender());
	}

}
