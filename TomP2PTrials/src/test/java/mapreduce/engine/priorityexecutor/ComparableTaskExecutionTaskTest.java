package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.tasks.Task;
import net.tomp2p.peers.Number160;

public class ComparableTaskExecutionTaskTest {
	private static Logger logger = LoggerFactory.getLogger(ComparableTaskExecutionTaskTest.class);

	@Test
	public void testNonFinished() {
		sortingTest(3, new int[] { 5, 4, 3, 2, 1, 0 });
	}

	@Test
	public void testSomeFinished() {
		sortingTest(2, new int[] { 5, 4, 2, 1, 3, 0 });
	}

	private void sortingTest(int numberOfResultHashs, int[] idOrder) {
		List<Task> tasks = new ArrayList<>();
		for (int i = 0; i < 6; ++i) {
			tasks.add((Task) Task.create(i + "", "E1").nrOfSameResultHash(numberOfResultHashs)
					.needsMultipleDifferentExecutors(false));
		}

		// FINISHED
		JobProcedureDomain jpd = JobProcedureDomain.create("J1", 0, "E1", "P1", 0);
		tasks.get(0).addOutputDomain(
				ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, jpd).resultHash(Number160.ZERO));
		tasks.get(0).addOutputDomain(
				ExecutorTaskDomain.create(tasks.get(0).key(), "E2", 0, jpd).resultHash(Number160.ZERO));
		logger.info("task 0 has outputdomains: " + tasks.get(0));
		// NOT FINISHED, EXECUTING ENOUGH
		tasks.get(1).addOutputDomain(
				ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, jpd).resultHash(Number160.ZERO));
		tasks.get(1).incrementActiveCount();

		// NOT FINISHED, NO OTHERS EXECUTING
		tasks.get(2).addOutputDomain(
				ExecutorTaskDomain.create(tasks.get(0).key(), "E1", 0, jpd).resultHash(Number160.ZERO));

		// NO RESULT HASH YET, BUT EXECUTING MAX NUMBER OF TIMES
		tasks.get(3).incrementActiveCount();
		tasks.get(3).incrementActiveCount();

		// NO RESULT HASH YET, BUT EXECUTING ONCE
		tasks.get(4).incrementActiveCount();

		// NONE EXECUTING YET
		tasks.get(5);

		Collections.sort(tasks, new Comparator<Task>() {

			@Override
			public int compare(Task o1, Task o2) {

				int result = 0;
				if (!o1.isFinished() && !o2.isFinished()) {
					if (o1.canBeExecuted() && o2.canBeExecuted()) {
						if (o1.currentMaxNrOfSameResultHash() == o2.currentMaxNrOfSameResultHash()) {
							if (o1.activeCount() > o2.activeCount()) {
								return 1;
							} else if (o1.activeCount() < o2.activeCount()) {
								return -1;
							} else {
								return 0;
							}
						} else if (o1.currentMaxNrOfSameResultHash() < o2.currentMaxNrOfSameResultHash()) {
							return -1;
						} else {
							return 1;
						}

					} else if (!o1.canBeExecuted() && o2.canBeExecuted()) {
						return 1;
					} else if (o1.canBeExecuted() && !o2.canBeExecuted()) {
						return -1;
					} else {
						return 0;
					}
				} else if (o1.isFinished() && !o2.isFinished()) {
					result = 1;
				} else if (!o1.isFinished() && o2.isFinished()) {
					result = -1;
				} else {
					result = 0;
				}
				return result;
			}
		});

		logger.info("After sorting: " + tasks);
		for (int i = 0; i < tasks.size(); ++i) {
			assertEquals(idOrder[i], Integer.parseInt(tasks.get(i).key()));
		}

		PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(2);

		int count = 0;
		boolean[] trueValues = new boolean[6];

		for (int i = 0; i < 6; ++i) {
			trueValues[i] = false;
		}

		for (int i = 0; i < 6; ++i) {
			assertEquals(false, trueValues[i]);
		}
		for (Task task : tasks) {
			int counter = count++;
			executor.submit(new Runnable() {

				@Override
				public void run() {
					logger.info(idOrder[counter] + "," + Integer.parseInt(task.key()));
					trueValues[counter] = idOrder[counter] == Integer.parseInt(task.key());
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}, task);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < 6; ++i) {
			assertEquals(true, trueValues[i]);
		}
	}

}
