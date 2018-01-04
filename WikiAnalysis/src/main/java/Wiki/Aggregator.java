package Wiki;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.AbstractActor.Receive;
import akka.japi.pf.ReceiveBuilder;

public class Aggregator extends AbstractActor {

	// messages received
	public static class Start {
	}

	public static class Result {
		Map<String, Integer> result;

		public Result(HashMap<String, Integer> result) {
			this.result = result;
		}
	}

	HashMap<String, Integer> finalMap = new HashMap<String, Integer>();
	int completed = 0;
	int totalActors = 0;
	int titlesProcessed = 0;
	final int BATCH_SIZE = 1000;

	public void onStart(Start s) throws IOException {
		File file = new File("enwiki-latest-all-titles-in-ns0");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = "";
		while (line != null) {
			List<String> list = new ArrayList<String>();
			totalActors++;
			ActorRef consumer = getContext().actorOf(Props.create(Consumer.class), "consumer" + totalActors);
			for (int i = 0; i < BATCH_SIZE && line != null; i++) {
				line = bufferedReader.readLine();
				list.add(line);
				titlesProcessed++;
			}
			consumer.tell(new Consumer.CountMsg(list), getSelf());
		}
		fileReader.close();
	}

	public void onResult(Result res) {
		completed++;
		System.out.println("Got results from " + getSender().toString());
		Set<Map.Entry<String, Integer>> set = res.result.entrySet();
		for (Map.Entry<String, Integer> e : set) {
			String key = e.getKey();
			Integer value = e.getValue();
			if (finalMap.containsKey(key)) {
				value = finalMap.get(key) + value;
			}
			finalMap.put(key, value);
		}
		if (completed == totalActors) {
			// when all actors return, processing is complete
			Map<String, Integer> topTen = finalMap.entrySet().stream()
					.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(10).collect(Collectors
							.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
			System.out.println(topTen);
			System.out.println("titles processed " + titlesProcessed);
		}
	}

	@Override
	public Receive createReceive() {
		return new ReceiveBuilder().match(Start.class, this::onStart).match(Result.class, this::onResult).build();
	}

}
