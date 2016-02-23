package mapreduce.engine.componenttests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.PerformanceInfo;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class JobCalculationMsgConsumerExecutorTests {

	private IDHTConnectionProvider dhtCon;
	private JobCalculationBroadcastHandler bcHandler;
	private JobCalculationMessageConsumer msgConsumer;
	private JobCalculationExecutor executor;

	private HashMap<ExecutorTaskDomain, String> initialETDsAndTaskValues = new HashMap<>();
	private Job job;
	private JobProcedureDomain initialInputDomain;

	@Before
	public void setUp() throws Exception {
		dhtCon = TestUtils.getTestConnectionProvider(null);
		bcHandler = Mockito.mock(JobCalculationBroadcastHandler.class);
//		dhtCon.broadcastHandler(bcHandler);
//		executor = JobCalculationExecutor.create().dhtConnectionProvider(dhtCon);
		msgConsumer = JobCalculationMessageConsumer.create().dhtConnectionProvider(dhtCon)
//				.executor(executor)
				;
		job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create());
	}

	@After
	public void tearDown() throws Exception {
		dhtCon.shutdown();
	}

	@Test
	public void testEverythingFromSameExecutor() throws Exception {
		int nrOfFiles = 10;
		int nrOfWordsPerFile = 10;
		addDHTData(dhtCon, job, nrOfFiles, nrOfWordsPerFile);

		initialInputDomain = JobProcedureDomain.create(job.id(), job.submissionCount(), "S1", DomainProvider.INITIAL_PROCEDURE, -1, 0).expectedNrOfFiles(nrOfFiles);

		JobProcedureDomain executorDomain = JobProcedureDomain.create(job.id(), job.submissionCount(), JobCalculationExecutor.classId, job.currentProcedure().executable().getClass().getSimpleName(),
				job.currentProcedure().procedureIndex(), job.currentProcedure().currentExecutionNumber()).resultHash(Number160.ZERO);

		List<ExecutorTaskDomain> initialETDs = new ArrayList<>();
		initialETDs.addAll(initialETDsAndTaskValues.keySet());
		for (int i = 0; i < initialETDsAndTaskValues.keySet().size(); ++i) {
			msgConsumer.handleCompletedTask(job, initialETDs.subList(i, (i + 1)), initialInputDomain);
			Thread.sleep(50); // Wait for data to be transferred from etd to jpd
			// The only thing that should happen here is that the data is transferred from etd to this executors jpd. Thus, tasks should also not be retrieved or executed
			FutureGet getJPD = dhtCon.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, executorDomain.toString()).awaitUninterruptibly();
			if (getJPD.isSuccess()) {
				assertEquals((i + 1), getJPD.dataMap().size());
				for (Number640 n : getJPD.dataMap().keySet()) {
					String taskKey = (String) getJPD.dataMap().get(n).object();
					boolean contains = false;
					for (ExecutorTaskDomain e : initialETDs) {
						if (e.taskId().equals(taskKey)) {
							contains = true;
						}
					}
					assertEquals(true, contains);
					FutureGet getValuesForKeys = dhtCon.getAll(taskKey, executorDomain.toString()).awaitUninterruptibly();
					if (getValuesForKeys.isSuccess()) {
						assertEquals(1, getValuesForKeys.dataMap().size());
						for (Number640 n2 : getValuesForKeys.dataMap().keySet()) {
							String taskValues = (String) ((Value) getValuesForKeys.dataMap().get(n2).object()).value();
							assertEquals(initialETDsAndTaskValues.get(getTaskKey(taskKey)), taskValues);
						}
					} else {
						fail();
					}
				}
			} else {
				fail();
			}
			System.err.println(job.currentProcedure().tasksSize());
			// Additionally, there should be no tasks retrieved from the dht, thus procedure should not have more tasks
			assertEquals((i + 1), job.currentProcedure().tasksSize());
		}
		msgConsumer.handleCompletedProcedure(job, executorDomain.expectedNrOfFiles(10), initialInputDomain);
		assertEquals(WordCountMapper.class.getSimpleName(), job.currentProcedure().executable().getClass().getSimpleName());
		// Adding msgs from external completed tasks
		JobProcedureDomain other = JobProcedureDomain
				.create(job.id(), job.submissionCount(), "E2", StartProcedure.class.getSimpleName(), job.currentProcedure().procedureIndex(), job.currentProcedure().currentExecutionNumber())
				.expectedNrOfFiles(nrOfWordsPerFile);

		List<ExecutorTaskDomain> etds = new ArrayList<>();
		 etds.add(ExecutorTaskDomain.create("task_1", "E2", 0, other).resultHash(Number160.ZERO));
		 etds.add(ExecutorTaskDomain.create("task_2", "E2", 0, other).resultHash(Number160.ZERO));
		 etds.add(ExecutorTaskDomain.create("task_3", "E2", 0, other).resultHash(Number160.ZERO));
		 etds.add(ExecutorTaskDomain.create("task_4", "E2", 0, other).resultHash(Number160.ZERO));
		 msgConsumer.handleCompletedTask(job, etds.subList(0, 1), job.currentProcedure().dataInputDomain());
		 msgConsumer.handleCompletedTask(job, etds.subList(1, 2), job.currentProcedure().dataInputDomain());
		 msgConsumer.handleCompletedTask(job, etds.subList(2, 3), job.currentProcedure().dataInputDomain());
		 msgConsumer.handleCompletedTask(job, etds.subList(3, 4), job.currentProcedure().dataInputDomain());
		msgConsumer.handleCompletedProcedure(job, JobProcedureDomain
				.create(job.id(), job.submissionCount(), "E2", job.currentProcedure().executable().getClass().getSimpleName(), job.currentProcedure().procedureIndex(), 0).resultHash(Number160.ZERO),
				job.currentProcedure().dataInputDomain());
		Thread.sleep(10000);
	}

	@Test
	public void testReceiveOldMessageDiscard() throws Exception {
		JobProcedureDomain executorDomain = JobProcedureDomain
				.create(job.id(), job.submissionCount(), JobCalculationExecutor.classId, StartProcedure.class.getSimpleName(), job.currentProcedure().procedureIndex(), job.currentProcedure().currentExecutionNumber())
				.resultHash(Number160.ZERO);
		// JobProcedureDomain otherExecutorDomain = JobProcedureDomain
		// .create(job.id(), job.submissionCount(), "E2", StartProcedure.class.getSimpleName(), job.currentProcedure().procedureIndex(), job.currentProcedure().currentExecutionNumber())
		// .resultHash(Number160.ZERO);
		testEverythingFromSameExecutor();
		// Assume e1 is better than e2 --> input domain stays same
		// // ALthough the procedure finished, we receive a result from another domain... should be ignored
		// PerformanceInfo p1 = Mockito.mock(PerformanceInfo.class);
		// PerformanceInfo p2 = Mockito.mock(PerformanceInfo.class);
		//
		// Mockito.when(p1.compareTo(p2)).thenReturn(-1);
		// executor.performanceInformation(p1);
		// otherExecutorDomain.executorPerformanceInformation(p2);

		msgConsumer.handleCompletedProcedure(job, executorDomain, initialInputDomain);
		assertEquals(executorDomain, job.currentProcedure().dataInputDomain());

		// // Assume e2 is better than e1 --> input domain is adapted to e2's
		// Mockito.when(p1.compareTo(p2)).thenReturn(1);
		// msgConsumer.handleCompletedProcedure(job, otherExecutorDomain, initialInputDomain);
		// assertEquals(otherExecutorDomain, job.currentProcedure().dataInputDomain());

	}

	private ExecutorTaskDomain getTaskKey(String taskKey) {
		for (ExecutorTaskDomain e : initialETDsAndTaskValues.keySet()) {
			if (e.taskId().equals(taskKey)) {
				return e;
			}
		}
		return null;
	}

	public void addDHTData(IDHTConnectionProvider dhtCon, Job job, int nrOfFiles, int nrOfWordsPerFile) {
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), job.submissionCount(), "S1", procedure.executable().getClass().getSimpleName(), procedure.procedureIndex(),
				procedure.currentExecutionNumber());

		for (int i = 0; i < nrOfFiles; ++i) {
			String values = "";
			for (int j = 0; j < nrOfWordsPerFile; ++j) {
				values += "word_" + j + " ";
			}
			ExecutorTaskDomain td = ExecutorTaskDomain.create("task_" + i, "S1", 0, outputJPD).resultHash(Number160.ZERO);

			initialETDsAndTaskValues.put(td, values);
			dhtCon.add(td.taskId(), values, td.toString(), true).awaitUninterruptibly();
			dhtCon.add(DomainProvider.TASK_OUTPUT_RESULT_KEYS, td.taskId(), td.toString(), false).awaitUninterruptibly();
		}
	}
}
