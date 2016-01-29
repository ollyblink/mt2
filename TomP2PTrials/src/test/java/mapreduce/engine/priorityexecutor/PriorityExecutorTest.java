package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.multithreading.ComparableTaskExecutionTask;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.utils.SyncedCollectionProvider;

public class PriorityExecutorTest {
	private static Logger logger = LoggerFactory.getLogger(PriorityExecutorTest.class);

	@Test
	public void testTaskSubmission() {
		// TODO How should I test this...
		// fail();
		//
		// ===========================================================================================================================================
		// This test should show if cancellation of Futures works (see JobCalculationMessageConsumer for the
		// actual methods that cancel procedures and
		// tasks and JobCalculationMessageConsumerTest for the tests of the corresponding methods). This is a
		//
		// continuation of the tests there.
		//
		// ===========================================================================================================================================
		PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(8);
		List<Task> tasks = new ArrayList<>();
		tasks.add(Task.create("hello", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("world", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("this", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("is", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("a", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("test", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("hello", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("world", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("this", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("is", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("a", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("test", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("hello", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("world", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("this", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("is", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("a", "E1").nrOfSameResultHash(1));
		tasks.add(Task.create("test", "E1").nrOfSameResultHash(1));

		String inputDomain = "JPD1";

		Map<String, ListMultimap<Task, Future<?>>> futures = SyncedCollectionProvider.syncedHashMap();

		// Submitting
		for (Task task : tasks) {
			addTaskFuture(inputDomain, task, executor.submit(new Runnable() {

				@Override
				public void run() {
					try {
						logger.info("Starting: " + task.key());
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}, task), futures);
		}
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//
		// ===========================================================================================================================================
		// The test expects that only the first task is executed and, as execution takes 3 seconds, other
		// tasks in the queue are aborted and, therefore, not executed anymore if they correspond to the
		// specified procedure or task. Each task should occur once in the queue because it needs 1 result
		// hash to be completed
		//
		// ===========================================================================================================================================
		// Test task abortion
		// while (!tasks.isEmpty()) {
		// for (int i = 0; i < tasks.size(); ++i) {
		// assertEquals(false, futures.get(inputDomain).get(tasks.get(i)).get(0).isCancelled());
		// }
		// cancelTaskExecution(inputDomain, tasks.get(0), futures);
		//
		// assertEquals(0, futures.get(inputDomain).get(tasks.get(0)).size());
		// tasks.remove(0);
		// assertEquals(tasks.size(), executor.getQueue().size());
		// }
	}

	//
	public void cancelTaskExecution(String dataInputDomainString, Task task, Map<String, ListMultimap<Task, Future<?>>> futures) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(dataInputDomainString);
		if (procedureFutures != null) {
			List<Future<?>> taskFutures = procedureFutures.get(task);
			for (Future<?> taskFuture : taskFutures) {
				taskFuture.cancel(true);
			}
			procedureFutures.get(task).clear();
		}
	}

	private void addTaskFuture(String dataInputDomainString, Task task, Future<?> taskFuture, Map<String, ListMultimap<Task, Future<?>>> futures) {
		ListMultimap<Task, Future<?>> taskFutures = futures.get(dataInputDomainString);
		if (taskFutures == null) {
			taskFutures = SyncedCollectionProvider.syncedArrayListMultimap();
			futures.put(dataInputDomainString, taskFutures);
		}
		taskFutures.put(task, taskFuture);
	}

	public void cancelProcedureExecution(Procedure procedure, Map<String, ListMultimap<Task, Future<?>>> futures) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(procedure.dataInputDomain().toString());
		if (procedureFutures != null) {
			for (Future<?> taskFuture : procedureFutures.values()) {
				taskFuture.cancel(true);
			}
			procedureFutures.clear();
		}
	}

	@Test
	public void testMessageSubmission() {
		// TODO How should I test this...
		// fail();
	}
}
