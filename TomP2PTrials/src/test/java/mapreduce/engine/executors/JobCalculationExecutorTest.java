package mapreduce.engine.executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
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
	private static JobCalculationExecutor jobExecutor;
	private static IDHTConnectionProvider dhtConnectionProvider;
	private static Job job;

	@BeforeClass
	public static void init() throws InterruptedException {
		dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1);

		JobCalculationBroadcastHandler handler = Mockito.mock(JobCalculationBroadcastHandler.class);
		dhtConnectionProvider.broadcastHandler(handler);

		jobExecutor = JobCalculationExecutor.create();
		jobExecutor.dhtConnectionProvider(dhtConnectionProvider);
		job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE)
				.addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);

	}

	@AfterClass
	public static void tearDown() throws InterruptedException {
		dhtConnectionProvider.shutdown();
	}

	@Test
	public void testSwitchDataFromTaskToProcedureDomain() throws InterruptedException {

		job.incrementProcedureIndex();
		Procedure procedure = job.currentProcedure();
		String executor = "Executor_1";
		Task task = Task.create("file1", executor);
		JobProcedureDomain inputJPD = JobProcedureDomain
				.create(job.id(), 0, executor, StartProcedure.class.getSimpleName(), 0).expectedNrOfFiles(1);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), 0, executor,
				WordCountMapper.class.getSimpleName(), 1);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.newStatusIndex(),
				outputJPD);

		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD)
				.dhtConnectionProvider(dhtConnectionProvider);
		procedure.dataInputDomain(inputJPD).addTask(task);

		Map<String, Integer> toCheck = new HashMap<>();
		for (int i = 0; i < 1000; ++i) {
			String next = (i % 5 == 0 ? "where"
					: (i % 4 == 0 ? "is" : (i % 3 == 0 ? "hello" : (i % 2 == 0 ? "world" : "test"))));
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
		FutureDone<List<FuturePut>> future = Futures.whenAllSuccess(context.futurePutData())
				.awaitUninterruptibly();

		if (future.isSuccess()) {
			jobExecutor.switchDataFromTaskToProcedureDomain(procedure, task);
		} else {
			logger.info("No success");
		}
		Thread.sleep(2000);
		assertEquals(true, task.isFinished());
		assertEquals(true, task.isInProcedureDomain());
		JobProcedureDomain jobDomain = JobProcedureDomain.create(job.id(), 0, jobExecutor.id(),
				WordCountMapper.class.getSimpleName(), 1);

		checkDHTValues(dhtConnectionProvider, toCheck, jobDomain);

	}

	private void checkDHTValues(IDHTConnectionProvider dhtConnectionProvider, Map<String, Integer> toCheck,
			IDomain jobDomain) {
		FutureGet futureGet = dhtConnectionProvider
				.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, jobDomain.toString())
				.awaitUninterruptibly();

		if (futureGet.isSuccess()) {
			try {
				Set<Number640> keys = futureGet.dataMap().keySet();
				assertEquals(5, keys.size());
				for (Number640 key : keys) {
					String value = (String) futureGet.dataMap().get(key).object();
					assertEquals(true, toCheck.containsKey(value));
					logger.info("testSwitchDataFromTaskToProcedureDomain():toCheck.containsKey(" + value
							+ ")?" + (toCheck.containsKey(value)));
					dhtConnectionProvider.getAll(value, jobDomain.toString()).awaitUninterruptibly()
							.addListener(new BaseFutureAdapter<FutureGet>() {

								@Override
								public void operationComplete(FutureGet future) throws Exception {
									if (future.isSuccess()) {
										Set<Number640> keys = future.dataMap().keySet();
										assertEquals(toCheck.get(value).intValue(), keys.size());
										logger.info("testSwitchDataFromTaskToProcedureDomain():toCheck.get("
												+ value + ").intValue() == " + keys.size() + "?"
												+ (toCheck.get(value).intValue() == keys.size()));
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
	public void testExecuteTaskWithoutCombiner() throws InterruptedException {
		testExecuteTask("test is test is test is test is is is test test test", new String[] { "test", "is" },
				null, 7, 6, 7, 6);
	}

	@Test
	public void testExecuteTaskWithCombiner() throws InterruptedException {
		testExecuteTask("a a b b b b b b a a a b a b a b a b a", new String[] { "a", "b" },
				WordCountReducer.create(), 1, 1, 9, 10);
	}

	private void testExecuteTask(String testIsText, String[] strings, IExecutable combiner, int testCount,
			int isCount, int testSum, int isSum) throws InterruptedException {

		JobProcedureDomain dataDomain = JobProcedureDomain
				.create(job.id(), 0, jobExecutor.id(), StartProcedure.class.getSimpleName(), 0)
				.expectedNrOfFiles(1);
		addTaskDataToProcedureDomain(dhtConnectionProvider, "file1", testIsText, dataDomain.toString());
		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).dataInputDomain(dataDomain)
				.combiner(combiner);

		jobExecutor.executeTask(Task.create("file1", "E1"), procedure);

		Thread.sleep(2000);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(procedure.dataInputDomain().jobId(), 0,
				jobExecutor.id(), procedure.executable().getClass().getSimpleName(), 1).nrOfFinishedTasks(1);
		Number160 resultHash = Number160.ZERO;
		if (combiner == null) {
			for (int i = 0; i < testSum; ++i) {
				resultHash = resultHash.xor(Number160.createHash(strings[0]))
						.xor(Number160.createHash(new Integer(1).toString()));
			}
			for (int i = 0; i < isSum; ++i) {
				resultHash = resultHash.xor(Number160.createHash(strings[1]))
						.xor(Number160.createHash(new Integer(1).toString()));
			}
		} else {
			resultHash = resultHash.xor(Number160.createHash(strings[0]))
					.xor(Number160.createHash(new Integer(testSum).toString()));
			resultHash = resultHash.xor(Number160.createHash(strings[1]))
					.xor(Number160.createHash(new Integer(isSum).toString()));
		}
		logger.info("Expected result hash: " + resultHash);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create("file1", jobExecutor.id(), 0, outputJPD)
				.resultHash(resultHash);

		// assertEquals(1, bcMessages.size());
		// CompletedBCMessage msg = (CompletedBCMessage) bcMessages.take();
		// assertEquals(BCMessageStatus.COMPLETED_TASK, msg.status());
		// assertEquals(dataDomain, msg.inputDomain());
		// assertEquals(outputETD, msg.outputDomain());

		logger.info("Output ExecutorTaskDomain: " + outputETD.toString());
		dhtConnectionProvider.getAll(strings[0], outputETD.toString()).awaitUninterruptibly()
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();
							assertEquals(testCount, keySet.size());
							int sum = 0;
							for (Number640 keyHash : keySet) {
								sum += (Integer) ((Value) future.dataMap().get(keyHash).object()).value();

							}
							assertEquals(testSum, sum);
							logger.info("test: " + sum);
						}
					}

				});
		dhtConnectionProvider.getAll(strings[1], outputETD.toString()).awaitUninterruptibly()
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();
							assertEquals(isCount, keySet.size());
							int sum = 0;
							for (Number640 keyHash : keySet) {
								sum += (Integer) ((Value) future.dataMap().get(keyHash).object()).value();

							}
							assertEquals(isSum, sum);
							logger.info("is: " + sum);
						}
					}

				});
		// Thread.sleep(2000);
	}

	private void addTaskDataToProcedureDomain(IDHTConnectionProvider dhtConnectionProvider, Object keyOut,
			Object valueOut, String oETDString) {
		List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
		futurePutData.add(dhtConnectionProvider.add(keyOut.toString(), valueOut, oETDString, true)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info(" Successfully performed add(" + keyOut.toString() + ", "
									+ valueOut.toString() + ").domain(" + oETDString + ")");
						} else {
							logger.info("Failed to perform add(" + keyOut.toString() + ", "
									+ valueOut.toString() + ").domain(" + oETDString + ")");
						}
					}
				}));
		futurePutData.add(dhtConnectionProvider
				.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, keyOut.toString(), oETDString, false)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Successfully performed add("
									+ DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS + ", " + keyOut.toString()
									+ ").domain(" + oETDString + ")");
						} else {

							logger.warn("Failed to perform add(" + DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS
									+ ", " + keyOut.toString() + ").domain(" + oETDString + ")");
						}
					}
				}));
		Futures.whenAllSuccess(futurePutData).awaitUninterruptibly();
	}

	@Test
	public void testTryFinishProcedure() {
		JobProcedureDomain dataDomain = JobProcedureDomain
				.create(job.id(), 0, jobExecutor.id(), StartProcedure.class.getSimpleName(), 0)
				.expectedNrOfFiles(1);

		Procedure procedure = Procedure.create(WordCountMapper.create(), 1).dataInputDomain(dataDomain)
				.nrOfSameResultHash(1).needsMultipleDifferentExecutors(false).nrOfSameResultHashForTasks(1)
				.needsMultipleDifferentExecutorsForTasks(false);

		jobExecutor.tryCompletingProcedure(procedure);
		assertEquals(false, procedure.isFinished());

		Task task1 = Task.create("hello", "E1");
		procedure.addTask(task1);
		jobExecutor.tryCompletingProcedure(procedure);
		assertEquals(false, procedure.isFinished());

		JobProcedureDomain jpd = JobProcedureDomain.create("J1", 0, "E1", "P1", 1);
		task1.addOutputDomain(ExecutorTaskDomain.create(task1.key(), "E1", 0, jpd));
		task1.isInProcedureDomain(true);
	}
}
