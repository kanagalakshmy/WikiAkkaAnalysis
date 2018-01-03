package Wiki;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import Wiki.Consumer.CountMsg;
import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;

public class Consumer extends AbstractActor {

	// insignificant words to be skipped
	static final List<String> skip = Arrays.asList(
			new String[] { "", "disambiguation", "s", "th", "the", "of", "in", "is", "and", "a", "for", "on", "from" });

	// input list to be consumed
	public static class CountMsg {
		List<String> titles;

		CountMsg(List<String> titles) {
			this.titles = titles;
		}
	}

	private void count(CountMsg m) {
		HashMap<String, Integer> countMap = new HashMap<String, Integer>();
		for (String title : m.titles) {
			title = title.toLowerCase();
			String[] words = title.split("[\\s\\.$:_!()\"\'1234567890+-]+");// delimiters, also skipping numbers
			for (String word : words) {
				if (!skip.contains(word)) {
					Integer count = countMap.get(word);
					Integer newCount = (count == null) ? 1 : count + 1;
					countMap.put(word, newCount);
				}
			}
		}
		getSender().tell(new Aggregator.Result(countMap), getSelf());

	}

	@Override
	public Receive createReceive() {
		return new ReceiveBuilder().match(CountMsg.class, this::count).build();
	}

}
