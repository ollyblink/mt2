package mapreduce.engine.executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class JobCalculationExecutorTest {
	protected static Logger logger = LoggerFactory.getLogger(JobCalculationExecutorTest.class);
	private static Random random = new Random();
	// private JobCalculationExecutor jobExecutor;
	private IDHTConnectionProvider dhtConnectionProvider;
	private Job job;

	@Before
	public void init() throws InterruptedException {
		dhtConnectionProvider = TestUtils.getTestConnectionProvider(null);

		JobCalculationBroadcastHandler bcHandler = Mockito.mock(JobCalculationBroadcastHandler.class);
		dhtConnectionProvider.broadcastHandler(bcHandler);
		// jobExecutor = JobCalculationExecutor.create();
		// jobExecutor.dhtConnectionProvider(dhtConnectionProvider);
		job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE).addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false, 0.0).addSucceedingProcedure(WordCountReducer.create(), null,
				1, 1, false, false, 0.0);

		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
	}

	@After
	public void tearDown() throws InterruptedException {
		dhtConnectionProvider.shutdown();
	}

	@Test
	public void testSwitchDataFromTaskToProcedureDomain() throws InterruptedException {

		job.incrementProcedureIndex();
		Procedure procedure = job.currentProcedure();
		String executor = "Executor_1";
		Task task = Task.create("file1", executor);
		JobProcedureDomain inputJPD = JobProcedureDomain.create(job.id(), 0, executor, StartProcedure.class.getSimpleName(), 0, 0).expectedNrOfFiles(1);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), 0, executor, WordCountMapper.class.getSimpleName(), 1, 0);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.currentExecutionNumber(), outputJPD);

		DHTStorageContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
		procedure.dataInputDomain(inputJPD).addTask(task);

		Map<String, Integer> toCheck = new HashMap<>();
		for (int i = 0; i < 1000; ++i) {
			String next = (i % 5 == 0 ? "where" : (i % 4 == 0 ? "is" : (i % 3 == 0 ? "hello" : (i % 2 == 0 ? "world" : "test"))));
			Integer counter = toCheck.get(next);
			if (counter == null) {
				counter = 0;
				toCheck.put(next, counter);
			}
			counter++;
			toCheck.put(next, counter);

			context.write(next, new Integer(1));
		}

		task.addOutputDomain(outputETD.resultHash(context.resultHash()));
		procedure.dataInputDomain(inputJPD);
		FutureDone<List<FuturePut>> future = Futures.whenAllSuccess(context.futurePutData()).awaitUninterruptibly();

		if (future.isSuccess()) {
			JobCalculationExecutor.create().dhtConnectionProvider(dhtConnectionProvider).switchDataFromTaskToProcedureDomain(procedure, task);
		} else {
			logger.info("No success");
		}
		Thread.sleep(2000);
		// Now everything should be reset as the procedure is finished...
		assertEquals(false, task.isFinished());
		assertEquals(false, task.isInProcedureDomain());
		JobProcedureDomain jobDomain = JobProcedureDomain.create(job.id(), 0, JobCalculationExecutor.classId, WordCountMapper.class.getSimpleName(), 1, 0);

		checkDHTValues(dhtConnectionProvider, toCheck, jobDomain);

	}

	private void checkDHTValues(IDHTConnectionProvider dhtConnectionProvider, Map<String, Integer> toCheck, IDomain jobDomain) {
		FutureGet futureGet = dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, jobDomain.toString()).awaitUninterruptibly();

		if (futureGet.isSuccess()) {
			try {
				Set<Number640> keys = futureGet.dataMap().keySet();
				assertEquals(5, keys.size());
				for (Number640 key : keys) {
					String value = (String) futureGet.dataMap().get(key).object();
					assertEquals(true, toCheck.containsKey(value));
					logger.info("testSwitchDataFromTaskToProcedureDomain():toCheck.containsKey(" + value + ")?" + (toCheck.containsKey(value)));
					dhtConnectionProvider.getAll(value, jobDomain.toString()).awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								Set<Number640> keys = future.dataMap().keySet();
								assertEquals(toCheck.get(value).intValue(), keys.size());
								logger.info("testSwitchDataFromTaskToProcedureDomain():toCheck.get(" + value + ").intValue() == " + keys.size() + "?" + (toCheck.get(value).intValue() == keys.size()));
							} else {
								fail();
							}
						}
					});
				}
			} catch (Exception e) {
				e.printStackTrace();
				fail();
			}

		} else {
			fail();
		}
	}

	@Test
	public void testExecuteTaskWithoutCombiner() throws Exception {
		testExecuteTask("test is test is test is test is is is test test test", new String[] { "test", "is" }, null, 7, 6, 7, 6);
	}

	@Test
	public void testExecuteTaskWithCombiner() throws Exception {
		testExecuteTask("a a b b b b b b a a a b a b a b a b a", new String[] { "a", "b" }, WordCountReducer.create(), 1, 1, 9, 10);
	}

	@Test
	public void testExecuteTaskWithCombinerTaskSummarisation() throws Exception {
		alternateTaskSummarisationFactor(0.0, true, false, 20);
		alternateTaskSummarisationFactor(0.000001, false, false, 20);
		alternateTaskSummarisationFactor(0.00001, false, false, 20);
		alternateTaskSummarisationFactor(0.0001, false, false, 20);
		alternateTaskSummarisationFactor(0.001, false, false, 20);
		alternateTaskSummarisationFactor(0.01, false, false, 20);
		for (double i = 0.1; i < 1.0; i = i + 0.1) {
			alternateTaskSummarisationFactor(i, false, false, 20);
		}
		alternateTaskSummarisationFactor(1.0, false, true, 20);
	}

	private void alternateTaskSummarisationFactor(double taskSummarisationFactor, boolean addInputData, boolean removeInputData, int nrOfTasks) throws Exception {

		JobProcedureDomain dataDomain = JobProcedureDomain.create(job.id(), 0, JobCalculationExecutor.classId, WordCountMapper.class.getSimpleName(), 1, 0);
		List<Task> tasks = new ArrayList<>();
		for (int i = 0; i < nrOfTasks; ++i) {
			tasks.add(Task.create("t_" + i, JobCalculationExecutor.classId).nrOfSameResultHash(1));
		}
		Procedure wordcountReducer = job.procedure(2);
		wordcountReducer.dataInputDomain(dataDomain);
		wordcountReducer.taskSummarisationFactor(taskSummarisationFactor);
		for (int i = 0; i < tasks.size(); ++i) {
			if (addInputData) {
				int numberOfOutputValues = (i % 50) + 1;

				for (int j = 0; j < numberOfOutputValues; ++j) {
					dhtConnectionProvider.add(tasks.get(i).key(), 1, dataDomain.toString(), true).awaitUninterruptibly();
				}
				dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, tasks.get(i).key(), dataDomain.toString(), false).awaitUninterruptibly();
			}
			wordcountReducer.addTask(tasks.get(i));
		}
		// Before
		for (Task t : tasks) {
			FutureGet getData = dhtConnectionProvider.getAll(t.key(), dataDomain.toString()).awaitUninterruptibly();
			if (getData.isSuccess()) {
				Set<Number640> keySet = getData.dataMap().keySet();
				String values = "";
				for (Number640 n : keySet) {
					try {
						values += (Integer) ((Value) getData.dataMap().get(n).object()).value() + ", ";
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
				}
				logger.info(t.key() + ", " + values);
			} else {
				fail();
			}
		}

		Field intermediateField = JobCalculationExecutor.class.getDeclaredField("intermediate");
		intermediateField.setAccessible(true);
		JobCalculationExecutor jobExecutor = JobCalculationExecutor.create().dhtConnectionProvider(dhtConnectionProvider).numberOfExecutions(wordcountReducer.numberOfExecutions());
		ListMultimap<JobProcedureDomain, ExecutorTaskDomain> intermediate = (ListMultimap<JobProcedureDomain, ExecutorTaskDomain>) intermediateField.get(jobExecutor);
		Field submittedField = JobCalculationExecutor.class.getDeclaredField("submitted");
		submittedField.setAccessible(true);
		Map<JobProcedureDomain, Integer> submitted = (Map<JobProcedureDomain, Integer>) submittedField.get(jobExecutor);

		JobProcedureDomain outputDomain = JobProcedureDomain.create(job.id(), job.submissionCount(), JobCalculationExecutor.classId, WordCountReducer.class.getSimpleName(), 2, 0);
		Task task = null;
		while ((task = wordcountReducer.nextExecutableTask()) != null) {
			jobExecutor.executeTask(task, wordcountReducer, job);
			int intermediatelyStoredTaskResults = intermediate.get(outputDomain).size();
			// int submittedTaskCount = submitted.get(outputDomain);
			int factorisedSummarisationCount = (int) (wordcountReducer.tasksSize() * wordcountReducer.taskSummarisationFactor());
			// logger.info("(int)(Taskssize*factor= " + wordcountReducer.tasksSize() + "*" + wordcountReducer.taskSummarisationFactor() + ")=[" + factorisedSummarisationCount
			// + "] intermediatelyStoredTaskResults <= factorisedSummarisationCount?" + intermediatelyStoredTaskResults + "<=" + factorisedSummarisationCount + "?"
			// + (intermediatelyStoredTaskResults <= factorisedSummarisationCount));
			assertEquals(true, (intermediatelyStoredTaskResults <= factorisedSummarisationCount));
		}
		for (int i = 0; i < tasks.size(); ++i) {
			Task t = tasks.get(i);
			ExecutorTaskDomain etd = ExecutorTaskDomain.create(t.key(), JobCalculationExecutor.classId, t.currentExecutionNumber(), outputDomain);
			FutureGet getData = dhtConnectionProvider.getAll(t.key(), etd.toString()).awaitUninterruptibly();
			if (getData.isSuccess()) {
				Set<Number640> keySet = getData.dataMap().keySet();
				for (Number640 n : keySet) {
					try {
						Integer value = (Integer) ((Value) getData.dataMap().get(n).object()).value();
						logger.info(t.key() + ", " + value);
						Integer numberOfOutputValues = (i % 50) + 1;
						assertEquals(numberOfOutputValues, value);
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
				}
			} else {
				fail();
			}

			dhtConnectionProvider.removeAll(t.key(), etd.toString()).awaitUninterruptibly();
			dhtConnectionProvider.removeAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, etd.toString()).awaitUninterruptibly();
		}
		wordcountReducer.reset();
		wordcountReducer.clear();

		if (removeInputData) {
			for (Task t : tasks) {
				dhtConnectionProvider.removeAll(t.key(), dataDomain.toString()).awaitUninterruptibly();
				dhtConnectionProvider.removeAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataDomain.toString()).awaitUninterruptibly();
			}
		}
	}

	private void testExecuteTask(String testIsText, String[] strings, IExecutable combiner, int testCount, int isCount, int testSum, int isSum)
			throws InterruptedException, ClassNotFoundException, IOException {

		JobProcedureDomain dataDomain = JobProcedureDomain.create(job.id(), 0, JobCalculationExecutor.classId, StartProcedure.class.getSimpleName(), 0, 0).expectedNrOfFiles(1);
		addTaskDataToProcedureDomain(dhtConnectionProvider, "file1", testIsText, dataDomain.toString());
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).dataInputDomain(dataDomain).combiner(combiner);
		JobCalculationExecutor jobExecutor = JobCalculationExecutor.create().dhtConnectionProvider(dhtConnectionProvider);

		jobExecutor.executeTask(Task.create("file1", "E1").nrOfSameResultHash(1), procedure, job);

		Thread.sleep(2000);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(procedure.dataInputDomain().jobId(), 0, JobCalculationExecutor.classId, procedure.executable().getClass().getSimpleName(), 1, 0)
				.nrOfFinishedTasks(1);
		Number160 resultHash = Number160.ZERO;
		if (combiner == null) {
			for (int i = 0; i < testSum; ++i) {
				resultHash = resultHash.xor(Number160.createHash(strings[0])).xor(Number160.createHash(new Integer(1).toString()));
			}
			for (int i = 0; i < isSum; ++i) {
				resultHash = resultHash.xor(Number160.createHash(strings[1])).xor(Number160.createHash(new Integer(1).toString()));
			}
		} else {
			resultHash = resultHash.xor(Number160.createHash(strings[0])).xor(Number160.createHash(new Integer(testSum).toString()));
			resultHash = resultHash.xor(Number160.createHash(strings[1])).xor(Number160.createHash(new Integer(isSum).toString()));
		}
		logger.info("Expected result hash: " + resultHash);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create("file1", JobCalculationExecutor.classId, 0, outputJPD).resultHash(resultHash);

		logger.info("Output ExecutorTaskDomain: " + outputETD.toString());
		FutureGet getAllFuture = dhtConnectionProvider.getAll(strings[0], outputETD.toString()).awaitUninterruptibly();

		if (getAllFuture.isSuccess()) {
			Set<Number640> keySet = getAllFuture.dataMap().keySet();
			assertEquals(testCount, keySet.size());
			int sum = 0;
			for (Number640 keyHash : keySet) {
				sum += (Integer) ((Value) getAllFuture.dataMap().get(keyHash).object()).value();

			}
			assertEquals(testSum, sum);
			logger.info("test: " + sum);
		} else {
			fail();
		}
		FutureGet getAllFuture2 = dhtConnectionProvider.getAll(strings[1], outputETD.toString()).awaitUninterruptibly();

		if (getAllFuture2.isSuccess()) {
			Set<Number640> keySet = getAllFuture2.dataMap().keySet();
			assertEquals(isCount, keySet.size());
			int sum = 0;
			for (Number640 keyHash : keySet) {
				sum += (Integer) ((Value) getAllFuture2.dataMap().get(keyHash).object()).value();

			}
			assertEquals(isSum, sum);
			logger.info("is: " + sum);
		} else {
			fail();
		}
		// Thread.sleep(2000);
	}

	private void addTaskDataToProcedureDomain(IDHTConnectionProvider dhtConnectionProvider, Object keyOut, Object valueOut, String oETDString) {
		List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
		futurePutData.add(dhtConnectionProvider.add(keyOut.toString(), valueOut, oETDString, true).addListener(new BaseFutureAdapter<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info(" Successfully performed add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
				} else {
					logger.info("Failed to perform add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
				}
			}
		}));
		futurePutData.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, keyOut.toString(), oETDString, false).addListener(new BaseFutureAdapter<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Successfully performed add(" + DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS + ", " + keyOut.toString() + ").domain(" + oETDString + ")");
				} else {

					logger.warn("Failed to perform add(" + DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS + ", " + keyOut.toString() + ").domain(" + oETDString + ")");
				}
			}
		}));
		Futures.whenAllSuccess(futurePutData).awaitUninterruptibly();
	}

	@Test
	public void testTryFinishProcedure() {
		JobProcedureDomain dataDomain = JobProcedureDomain.create(job.id(), 0, JobCalculationExecutor.classId, StartProcedure.class.getSimpleName(), 0, 0).expectedNrOfFiles(1);

		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).dataInputDomain(dataDomain).nrOfSameResultHash(1).needsMultipleDifferentExecutors(false).nrOfSameResultHashForTasks(1)
				.needsMultipleDifferentExecutorsForTasks(false);
		JobCalculationExecutor jobExecutor = JobCalculationExecutor.create().dhtConnectionProvider(dhtConnectionProvider);

		jobExecutor.tryCompletingProcedure(procedure);
		assertEquals(false, procedure.isFinished());

		Task task1 = Task.create("hello", "E1");
		procedure.addTask(task1);
		jobExecutor.tryCompletingProcedure(procedure);
		assertEquals(false, procedure.isFinished());

		JobProcedureDomain jpd = JobProcedureDomain.create("J1", 0, "E1", "P1", 1, 0);
		task1.addOutputDomain(ExecutorTaskDomain.create(task1.key(), "E1", 0, jpd));
		task1.isInProcedureDomain(true);
	}

	@Test
	public void testAbortExecution() {
	}
}
