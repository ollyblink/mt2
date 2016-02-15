package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.peers.Number640;

public class PriorityExecutorTest {
	private static Logger logger = LoggerFactory.getLogger(PriorityExecutorTest.class);
	private IDHTConnectionProvider dhtConnectionProvider;
	// private static Random random = new Random();

	@Before
	public void setUp() {
		JobCalculationBroadcastHandler mockBCHandler = Mockito.mock(JobCalculationBroadcastHandler.class);
		dhtConnectionProvider = TestUtils.getTestConnectionProvider(null).broadcastHandler(mockBCHandler);
		mockBCHandler.dhtConnectionProvider(dhtConnectionProvider);
	}

	@After
	public void tearDown() {
		dhtConnectionProvider.shutdown();
	}

	@Test
	public void testTaskSubmissionAndAbortion() throws InterruptedException {

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountReducer.create(), null);
		JobCalculationExecutor jobExecutor = JobCalculationExecutor.create();
		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		JobProcedureDomain dataDomain = JobProcedureDomain.create(job.id(), 0, jobExecutor.id(), WordCountMapper.class.getSimpleName(), 1, 0);
		List<Task> tasks = new ArrayList<>();
		int nrOfTasks = 50;
		for (int i = 0; i < nrOfTasks; ++i) {
			tasks.add(Task.create("t_" + i, jobExecutor.id()).nrOfSameResultHash(1));
		}
		for (int i = 0; i < tasks.size(); ++i) {
			int numberOfOutputValues = (i % 50) + 1;
			for (int j = 0; j < numberOfOutputValues; ++j) {
				dhtConnectionProvider.add(tasks.get(i).key(), 1, dataDomain.toString(), true).awaitUninterruptibly();
			}
			dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, tasks.get(i).key(), dataDomain.toString(), false).awaitUninterruptibly();

		}

		Map<String, ListMultimap<Task, Future<?>>> futures = SyncedCollectionProvider.syncedHashMap();

		// Submitting
		PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(4);
		logger.info("BEFORE TEST");
		job.incrementProcedureIndex().currentProcedure().dataInputDomain(dataDomain);
		for (Task task : tasks) {
			Future<?> future = executor.submit(JobCalculationExecutor.create(task, job.currentProcedure(), job, true).dhtConnectionProvider(dhtConnectionProvider), task);
			addTaskFuture(dataDomain.toString(), task, future, futures);
		}
		// ===========================================================================================================================================
		// Now all tasks are aborted (through their futures). This does also abort the TomP2P futures inside the JobCalculationExecutor
		// ===========================================================================================================================================

		System.err.println("ABORT");
		new Thread(new Runnable() {

			@Override
			public void run() {
				ListMultimap<Task, Future<?>> listMultimap = futures.get(dataDomain.toString());
				for (Task t : listMultimap.keySet()) {
					List<Future<?>> list = listMultimap.get(t);
					for (Future<?> f : list) {
						f.cancel(true);
					}
				}
			}

		}).start();
		Thread.sleep(1000);
		JobProcedureDomain jobProcedureDomain = JobProcedureDomain.create(job.id(), job.submissionCount(), jobExecutor.id(), job.currentProcedure().executable().getClass().getSimpleName(), 1, 0);
		// ===========================================================================================================================================
		// I now simply assure that not all tasks finished because its impossible to say which tasks will finish and which will not...
		// ===========================================================================================================================================
		int all = tasks.size(); // Expected nr of finished tasks if all tasks finished
		int count = 0; // Count of all tasks that actually finished (assumed less than all)
		for (Task task : tasks) {
			ExecutorTaskDomain etd = ExecutorTaskDomain.create(task.key(), jobExecutor.id(), 0, jobProcedureDomain);
			FutureGet futureGet = dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, etd.toString()).awaitUninterruptibly();
			if (futureGet.isSuccess()) {
				Set<Number640> keySet = futureGet.dataMap().keySet();
				System.err.println("KeySet size for key (" + task.key() + ") from the DHT: " + keySet.size());
				for (Number640 n : keySet) {
					try {
						String key = (String) (futureGet.dataMap().get(n).object());
						System.err.println("Retrieved key from dht: " + key);
						FutureGet futureGet2 = dhtConnectionProvider.getAll(key, etd.toString()).awaitUninterruptibly();
						if (futureGet2.isSuccess()) {
							Set<Number640> keySet2 = futureGet2.dataMap().keySet();
							System.err.println("Task out vals size:" + keySet2.size());
							String values = "";
							for (Number640 n2 : keySet2) {
								try {
									values += (Integer) ((Value) futureGet2.dataMap().get(n2).object()).value() + ", ";
								} catch (ClassNotFoundException | IOException e) {
									e.printStackTrace();
								}
							}
							if (keySet.size() > 0 && keySet2.size() > 0) {
								// Only if there was received something from all, the count is increased...
								++count;
							}
							System.err.println(key + ", " + values);
						} else {
							fail();
						}
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
				}

			}
		}
		assertEquals(true, (count < all)); // I assume not all tasks finish if they are aborted mid-execution
		try {
			if (executor.awaitTermination(10, TimeUnit.MILLISECONDS)) {

			} else {
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread.sleep(2000);

	}

	@Ignore
	public void testMessageSubmission() {
		// TODO How should I test this...
		// fail();
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

}
