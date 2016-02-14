package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
	private static Random random = new Random();

	@Test
	public void testTaskSubmission() throws InterruptedException {
		JobCalculationBroadcastHandler mockBCHandler = Mockito.mock(JobCalculationBroadcastHandler.class);
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(null).broadcastHandler(mockBCHandler);
		mockBCHandler.dhtConnectionProvider(dhtConnectionProvider);

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
		// Before
//		for (Task t : tasks) {
//			FutureGet getData = dhtConnectionProvider.getAll(t.key(), dataDomain.toString()).awaitUninterruptibly();
//			if (getData.isSuccess()) {
//				Set<Number640> keySet = getData.dataMap().keySet();
//				String values = "";
//				for (Number640 n : keySet) {
//					try {
//						values += (Integer) ((Value) getData.dataMap().get(n).object()).value() + ", ";
//					} catch (ClassNotFoundException | IOException e) {
//						e.printStackTrace();
//					}
//				}
//				logger.info(t.key() + ", " + values);
//			} else {
//				fail();
//			}
//		}

		//
		// ===========================================================================================================================================

		// ===========================================================================================================================================

		Map<String, ListMultimap<Task, Future<?>>> futures = SyncedCollectionProvider.syncedHashMap();

		// Submitting
		PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(4);
		logger.info("BEFORE TEST");
		job.incrementProcedureIndex().currentProcedure().dataInputDomain(dataDomain);
		for (Task task : tasks) {
			Future<?> future = executor.submit(JobCalculationExecutor.create(task, job.currentProcedure(), job, true).dhtConnectionProvider(dhtConnectionProvider), task);
			addTaskFuture(dataDomain.toString(), task, future, futures);
		}
		//
		// ===========================================================================================================================================
		// The test expects that only the first task is executed and, as execution takes 3 seconds, other
		// tasks in the queue are aborted and, therefore, not executed anymore if they correspond to the
		// specified procedure or task. Each task should occur once in the queue because it needs 1 result
		// hash to be completed
		//
		// ===========================================================================================================================================

		new Thread(new Runnable() {

			@Override
			public void run() {
				logger.info("ABORT");
				ListMultimap<Task, Future<?>> listMultimap = futures.get(dataDomain.toString());
				for (Task t : listMultimap.keySet()) {
					// System.err.println("Task to abort: " + t);
//					if (t.key().equals(tasks.get(0))) {
//						continue;
//					}
					List<Future<?>> list = listMultimap.get(t);
					for (Future<?> f : list) {
						f.cancel(true);
					}
				}
//				try {
//					if (executor.awaitTermination(10, TimeUnit.MILLISECONDS)) {
//
//					} else {
//						executor.shutdownNow();
//					}
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
			}

		}).start();
		Thread.sleep(1000);
		JobProcedureDomain jobProcedureDomain = JobProcedureDomain.create(job.id(), job.submissionCount(), jobExecutor.id(), job.currentProcedure().executable().getClass().getSimpleName(), 1, 0);

		for (Task task : tasks) {
			ExecutorTaskDomain etd = ExecutorTaskDomain.create(task.key(), jobExecutor.id(), 0, jobProcedureDomain);
 			FutureGet futureGet = dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, etd.toString()).awaitUninterruptibly();
			if (futureGet.isSuccess()) {
				Set<Number640> keySet = futureGet.dataMap().keySet();
				System.err.println("Task out keys size: " + keySet.size());
				for (Number640 n : keySet) {
					try {
						String key = (String) (futureGet.dataMap().get(n).object());
						System.err.println("Key: " + key);
						FutureGet futureGet2 = dhtConnectionProvider.getAll(key, etd.toString()).awaitUninterruptibly();
						if (futureGet2.isSuccess()) {
							Set<Number640> keySet2 = futureGet2.dataMap().keySet();
							System.err.println("Task out vals size:" + keySet.size());
							String values = "";
							for (Number640 n2 : keySet2) {
								try {
									values += (Integer) ((Value) futureGet2.dataMap().get(n2).object()).value() + ", ";
								} catch (ClassNotFoundException | IOException e) {
									e.printStackTrace();
								}
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
		Thread.sleep(Long.MAX_VALUE);

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
