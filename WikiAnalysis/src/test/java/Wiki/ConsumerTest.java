package Wiki;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import Wiki.Aggregator;
import Wiki.Aggregator.Result;
import Wiki.Consumer;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;

public class ConsumerTest {

	static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create();
	}

	@AfterClass
	public static void teardown() {
		TestKit.shutdownActorSystem(system);
		system = null;
	}

	@Test
	public void testConsumer() {
		new TestKit(system) {
			{
				final ActorRef consumer = system.actorOf(Props.create(Consumer.class), "consumer");
				List<String> input = Arrays.asList(new String[] { "apple!apple!!bat_bat", "apple.bat$bat5345bat" });
				consumer.tell(new Consumer.CountMsg(input), getRef());

				HashMap<String, Integer> expectedMap = new HashMap<>();
				expectedMap.put("apple", 3);
				expectedMap.put("bat", 5);
				Result expected = new Result(expectedMap);
				Result actual = expectMsgClass(expected.getClass());

				Assert.assertTrue(expected.result.equals(actual.result));
			}
		};
	}
}