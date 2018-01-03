package Wiki;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;

public class Wiki {

	public static void main(String[] args) throws IOException {
		ActorSystem system = ActorSystem.create("wikiActors");

		ActorRef aggregator = system.actorOf(Props.create(Aggregator.class), "aggregator");
		aggregator.tell("start", ActorRef.noSender());
	}
}

class Aggregator extends AbstractActor {

	HashMap<String, Integer> finalMap = new HashMap<String, Integer>();
	int completed = 0;
	int totalActors = 0;
	int titlesProcessed = 0;

	@Override
	public Receive createReceive() {
		return new ReceiveBuilder().match(String.class, s -> {

			File file = new File("C:\\Users\\Divya\\Downloads\\enwiki-latest-all-titles-in-ns0");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = "";
			while (line != null) {
				List<String> list = new ArrayList<String>();
				totalActors++;
				ActorRef consumer = getContext().actorOf(Props.create(Consumer.class), "consumer" + totalActors);
				for (int i = 0; i < 10000 && line != null; i++) {
					line = bufferedReader.readLine();
					list.add(line);
					titlesProcessed++;
				}
				consumer.tell(new Consumer.CountMsg(list), getSelf());
			}
			fileReader.close();
		}).match(HashMap.class, m -> {
			completed++;
			System.out.println("got results from " + getSender().toString());
			Set<Map.Entry<String, Integer>> set = m.entrySet();
			for (Map.Entry<String, Integer> e : set) {
				String key = e.getKey();
				Integer value = e.getValue();
				if (finalMap.containsKey(key)) {
					value = finalMap.get(key) + value;
				}
				finalMap.put(key, value);
			}
			if (completed == totalActors) {
				Map<String, Integer> topTen = finalMap.entrySet().stream()
						.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(10).collect(Collectors
								.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
				System.out.println(topTen);
				System.out.println("titles processed " + titlesProcessed);
			}
		}).build();
	}

}

class Consumer extends AbstractActor {

	List<String> skip = Arrays.asList(
			new String[] { "disambiguation", "s", "th", "the", "of", "in", "is", "and", "a", "for", "on", "from" });

	static class CountMsg {
		List<String> titles;

		CountMsg(List<String> titles) {
			this.titles = titles;
		}
	}

	private void count(CountMsg m) {
		HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		for (String title : m.titles) {
			title = title.toLowerCase();
			String[] words = title.split("[\\s\\.$:_!()\"\'1234567890+-]+");
			for (String word : words) {
				if (!skip.contains(word)) {
					Integer count = countMap.get(word);
					Integer newCount = (count == null) ? 1 : count + 1;
					countMap.put(word, newCount);
				}
			}
		}
		getSender().tell(countMap, getSelf());
	}

	@Override
	public Receive createReceive() {

		return new ReceiveBuilder().match(CountMsg.class, this::count).build();
	}

}