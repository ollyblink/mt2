package mapreduce.engine.broadcasting.broadcasthandlers.timeouts;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.broadcasting.broadcasthandlers.timeout.AbstractTimeout;
import mapreduce.engine.broadcasting.broadcasthandlers.timeout.JobCalculationTimeout;
import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;

public class DynamicTimeoutTests {

	long first = 50;
	long second = 100;
	long initialTimeToLive = 300;
	private CompletedProcedureBCMessage bcMessage;

	@Before
	public void before() {
		// BCMessage
		bcMessage = Mockito.mock(CompletedProcedureBCMessage.class);
	}

	@Test
	public void testDynamicTimeout() throws Exception {
		Field timeToLiveField = AbstractTimeout.class.getDeclaredField("timeToLive");
		timeToLiveField.setAccessible(true);
		JobCalculationTimeout timeout = new JobCalculationTimeout(null, null, System.currentTimeMillis(), bcMessage, initialTimeToLive, true, 0.0);

		Thread t = new Thread(timeout);
		t.start();
		long timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive == initialTimeToLive);
		// First adapted time to live
		Thread.sleep(first);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive < first+(first/2) && timeToLive >= first);

		// larger adapted time to live
		Thread.sleep(second);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive < second+(second/2) && timeToLive >= second);

		// Smaller time to live --> no adaption, always takes max
		Thread.sleep(first);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive < second+(second/2) && timeToLive >= second);
	}

	@Test
	public void testFraction() throws Exception {
		double fraction = 0.5;
		Field timeToLiveField = AbstractTimeout.class.getDeclaredField("timeToLive");
		timeToLiveField.setAccessible(true);
		JobCalculationTimeout timeout = new JobCalculationTimeout(null, null, System.currentTimeMillis(), bcMessage, initialTimeToLive, true, fraction);

		Thread t = new Thread(timeout);
		t.start();

		long timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive == initialTimeToLive);
		// First adapted time to live
		Thread.sleep(first);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive < ((first+(first/2)) + fraction * (first+(first/2)) ) && timeToLive >= (first + fraction * first));

		// larger adapted time to live
		Thread.sleep(second);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive < ((second+(second/2)) + fraction * (second+(second/2)) ) && timeToLive >= (second + fraction * second));

		// Smaller time to live --> no adaption, always takes max
		Thread.sleep(first);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		timeToLive = (Long) timeToLiveField.get(timeout);
		assertEquals(true, timeToLive < ((second+(second/2)) + fraction * (second+(second/2)) ) && timeToLive >= (second + fraction * second));
	}

}
