package mapreduce.engine.messageconsumers;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.engine.messageconsumers.updates.IUpdate;
import mapreduce.engine.messageconsumers.updates.ProcedureUpdate;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.resultprinter.IResultPrinter;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.peers.Number160;

public class JobCalculationMessageConsumerTest {

	private static JobCalculationMessageConsumer calculationMsgConsumer;
	private static IDHTConnectionProvider mockDHT;
	private static JobCalculationExecutor calculationExecutor;

	@Before
	public void setUpBefore() throws Exception {
		mockDHT = Mockito.mock(IDHTConnectionProvider.class);
		// Calculation Executor
		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		Mockito.when(calculationExecutor.id()).thenReturn("E1");
		// Calculation MessageConsumer
		calculationMsgConsumer = JobCalculationMessageConsumer.create();
		calculationMsgConsumer.dhtConnectionProvider(mockDHT).executor(calculationExecutor);

	}

	@Test
	public void testTryIncrementProcedure() throws Exception {

		Method tryIncrementProcedureMethod = calculationMsgConsumer.getClass().getDeclaredMethod(
				"tryIncrementProcedure", Job.class, JobProcedureDomain.class, JobProcedureDomain.class);
		tryIncrementProcedureMethod.setAccessible(true);

		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);
		JobProcedureDomain firstInputDomain = JobProcedureDomain.create(job.id(), 0, "E1", "INITIAL", -1);
		JobProcedureDomain secondInputDomain = JobProcedureDomain.create(job.id(), 0, "E1",
				StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain thirdInputDomain = JobProcedureDomain.create(job.id(), 0, "E1",
				WordCountMapper.class.getSimpleName(), 1);
		JobProcedureDomain lastInputDomain = JobProcedureDomain.create(job.id(), 0, "E1",
				EndProcedure.class.getSimpleName(), 2);

		JobProcedureDomain firstOutputDomain = secondInputDomain;
		JobProcedureDomain secondOutputDomain = thirdInputDomain;
		JobProcedureDomain thirdOutputDomain = lastInputDomain;

		job.currentProcedure().dataInputDomain(firstInputDomain);
		job.currentProcedure().addOutputDomain(firstOutputDomain);

		// Currently: second procedure (Start procedure)
		job.incrementProcedureIndex();
		job.currentProcedure().dataInputDomain(secondInputDomain);
		job.currentProcedure().addOutputDomain(secondOutputDomain);

		// Incoming: result for procedure before: nothing happens
		String procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
		int procedureIndexBefore = job.currentProcedure().procedureIndex();
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, firstInputDomain, firstOutputDomain);
		String procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		int procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(procedureNameBefore, procedureNameAfter);
		assertEquals(procedureIndexBefore, procedureIndexAfter);
		assertEquals(secondInputDomain, job.currentProcedure().dataInputDomain());

		// Incoming: result for same procedure: nothing happens
		procedureNameBefore = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexBefore = job.currentProcedure().procedureIndex();
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, secondInputDomain,
				secondOutputDomain);
		procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(procedureNameBefore, procedureNameAfter);
		assertEquals(procedureIndexBefore, procedureIndexAfter);
		assertEquals(secondInputDomain, job.currentProcedure().dataInputDomain());

		// //Incoming: after: updates current procedure to the one received
		tryIncrementProcedureMethod.invoke(calculationMsgConsumer, job, thirdInputDomain, thirdOutputDomain);
		procedureNameAfter = job.currentProcedure().executable().getClass().getSimpleName();
		procedureIndexAfter = job.currentProcedure().procedureIndex();
		assertEquals(EndProcedure.class.getSimpleName(), procedureNameAfter);
		assertEquals(2, procedureIndexAfter);
		assertEquals(thirdInputDomain, job.currentProcedure().dataInputDomain());

	}

	@Test
	public void testTryUpdateTasksOrProcedures() throws Exception {

		Method tryUpdate = JobCalculationMessageConsumer.class.getDeclaredMethod("tryUpdateTasksOrProcedures",
				Job.class, JobProcedureDomain.class, IDomain.class, IUpdate.class);
		tryUpdate.setAccessible(true);

		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 1, false, false);
		job.incrementProcedureIndex(); // Currently: second procedure (Wordcount mapper)
		JobProcedureDomain in = JobProcedureDomain.create(job.id(), 0, "E1",
				StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain out = JobProcedureDomain.create(job.id(), 0, "E1",
				WordCountMapper.class.getSimpleName(), 1);
		IUpdate update = Mockito.mock(IUpdate.class);

		int local = 10;
		int receivedLess = 9;
		int receivedEqual = local;
		int receivedMore = 11;
		job.currentProcedure().dataInputDomain(in.expectedNrOfFiles(local));
		job.currentProcedure().addOutputDomain(out);

		// ===========================================================================================================================================
		// Same input domain: calls IUpdate.executeUpdate(), possible update of expectedNrOfFiles()
		// ===========================================================================================================================================
		// Here we want to check if the procedure's nr of expected files is updated when a new message comes
		// in that tells it that there are more
		// files (needed in case some files got lost somehow)

		// Less: no update
		JobProcedureDomain receivedIn = JobProcedureDomain
				.create(job.id(), 0, "E1", StartProcedure.class.getSimpleName(), 0)
				.expectedNrOfFiles(receivedLess);

		assertEquals(10, job.currentProcedure().dataInputDomain().expectedNrOfFiles());
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(10, job.currentProcedure().dataInputDomain().expectedNrOfFiles());
		Mockito.verify(update, Mockito.times(1)).executeUpdate(out, job.currentProcedure());

		// Same: no update
		receivedIn.expectedNrOfFiles(receivedEqual);
		assertEquals(10, job.currentProcedure().dataInputDomain().expectedNrOfFiles());
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(10, job.currentProcedure().dataInputDomain().expectedNrOfFiles());
		Mockito.verify(update, Mockito.times(2)).executeUpdate(out, job.currentProcedure());

		// more: update to 11
		receivedIn.expectedNrOfFiles(receivedMore);
		assertEquals(10, job.currentProcedure().dataInputDomain().expectedNrOfFiles());
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(11, job.currentProcedure().dataInputDomain().expectedNrOfFiles());
		Mockito.verify(update, Mockito.times(3)).executeUpdate(out, job.currentProcedure());

		// ===========================================================================================================================================
		// Different input domain: Call to changeDataInputDomain(), possible change of data input domain
		// ===========================================================================================================================================
		// Here it is evaluated which data input domain to use as it turned out that two different executors
		// use different data input domains for
		// their data execution. This may happen if not all receive all broadcast messages. The idea is that
		// the one that has fewer tasks finished
		// aborts its execution. The other one ignores the update. If they executed the same number of tasks
		// so far on different input domains, it has
		// to be decided which one should abort.
		in.nrOfFinishedTasks(local);

		// No adaption as there are less executed in the received one --> the other one has to adapt the data
		// input domain
		receivedIn = JobProcedureDomain.create(job.id(), 0, "E2", StartProcedure.class.getSimpleName(), 0)
				.nrOfFinishedTasks(receivedLess);
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(in, job.currentProcedure().dataInputDomain());

		// Adaption as there are more executed in the received one--> this one has to adapt the data input
		// domain
		receivedIn = JobProcedureDomain.create(job.id(), 0, "E2", StartProcedure.class.getSimpleName(), 0)
				.nrOfFinishedTasks(receivedMore);
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(receivedIn, job.currentProcedure().dataInputDomain());

		// Equals: Performance evaluator needs to be called...
		job.currentProcedure().dataInputDomain(in.nrOfFinishedTasks(local));
		receivedIn.nrOfFinishedTasks(local);
		PerformanceInfo thisPI = Mockito.mock(PerformanceInfo.class);
		Mockito.when(calculationExecutor.performanceInformation()).thenReturn(thisPI);
		PerformanceInfo otherPI = Mockito.mock(PerformanceInfo.class);
		receivedIn.executorPerformanceInformation(otherPI);
		Comparator<PerformanceInfo> performanceEvaluator = Mockito.mock(Comparator.class);
		calculationMsgConsumer.performanceEvaluator(performanceEvaluator);

		// Received is worse than this
		Mockito.when(performanceEvaluator.compare(thisPI, otherPI)).thenReturn(1);
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(in, job.currentProcedure().dataInputDomain());

		// Received is better than this
		job.currentProcedure().dataInputDomain(in);
		Mockito.when(performanceEvaluator.compare(thisPI, otherPI)).thenReturn(-1);
		tryUpdate.invoke(calculationMsgConsumer, job, receivedIn, out, update);
		assertEquals(receivedIn, job.currentProcedure().dataInputDomain());
	}

	@Test
	public void testTryExecuteProcedure() throws Exception {
		Method tryExecuteProcedure = JobCalculationMessageConsumer.class
				.getDeclaredMethod("tryExecuteProcedure", Job.class);
		tryExecuteProcedure.setAccessible(true);

		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 0, false, false);

		JobProcedureDomain in = JobProcedureDomain.create(job.id(), 0, "E1",
				StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain out = JobProcedureDomain.create(job.id(), 0, "E1",
				WordCountMapper.class.getSimpleName(), 1).resultHash(Number160.ONE);
		job.currentProcedure().nrOfSameResultHash(0);
		job.currentProcedure().nrOfSameResultHashForTasks(0);
		job.incrementProcedureIndex(); // Currently: second procedure (Wordcount mapper)

		// ===========================================================================================================================================
		// Job is NOT finished: Needs to be evaluated if all tasks have already been retrieved
		// ===========================================================================================================================================
		// Case 1: if the received nr of expected tasks is larger than the current task size more tasks may be
		// retrieved from the dht. A special case
		// is the StartProcedure: as tasks are only retrieved with broadcast (they are not put into dht
		// directly when JobSubmissionExecutor.submit()
		// is called), they cannot be retrieved from DHT. Thus, if the current procedure is the StartProcedure
		// (procedureIndex == 0 always), then
		// nothing should happen as these tasks do not need to be executed but only transferred once all tasks
		// for the StartProcedure have been
		// received via broadcast
		// Input: nr of tasks in current procedure is 0
		// Expected nr of tasks is 1
		job.currentProcedure().dataInputDomain(in.expectedNrOfFiles(1));

		Field boolsForRetrieving = JobCalculationMessageConsumer.class
				.getDeclaredField("currentlyRetrievingTaskKeysForProcedure");
		boolsForRetrieving.setAccessible(true);
		Map<String, Boolean> field = (Map<String, Boolean>) boolsForRetrieving.get(calculationMsgConsumer);
		FutureGet mockFuture = Mockito.mock(FutureGet.class);
		Mockito.when(mockFuture.awaitUninterruptibly()).thenReturn(mockFuture);
		Mockito.when(mockDHT.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS,
				job.currentProcedure().dataInputDomain().toString()))
				.thenReturn(mockFuture);
		

		assertEquals(null, field.get(job.currentProcedure().dataInputDomain().toString()));
		tryExecuteProcedure.invoke(calculationMsgConsumer, job);
		assertEquals(true, field.get(job.currentProcedure().dataInputDomain().toString())); // It's not
																							// removed anymore
																							// because there
																							// is no actual
																							// listener
		Mockito.verify(mockDHT, Mockito.times(1)).getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS,
				job.currentProcedure().dataInputDomain().toString());

		// Case 2: it seems that the other executor thinks there are the same number of tasks: Simply try to
		// execute the tasks by adding them to the
		// queue for scheduling
		Task task = Mockito.mock(Task.class);
		Mockito.when(task.canBeExecuted()).thenReturn(false);
		job.currentProcedure().addTask(task);
		tryExecuteProcedure.invoke(calculationMsgConsumer, job);
		Mockito.verify(task, Mockito.times(1)).canBeExecuted(); // This means the task was tried to submit,
																// which only happens if the else-if part is
																// called (except for when the dht is called
																// but this cannot happen here as it is only
																// a mock dht

		// ===========================================================================================================================================
		// Job is finished: should simply print results to output
		// ===========================================================================================================================================
 		ProcedureUpdate update = new ProcedureUpdate(job, calculationMsgConsumer);
		update.executeUpdate(out, job.currentProcedure());
		IResultPrinter resultPrinter = Mockito.mock(IResultPrinter.class);
		calculationMsgConsumer.resultPrinter(resultPrinter);
		tryExecuteProcedure.invoke(calculationMsgConsumer, job);
		Mockito.verify(resultPrinter, Mockito.timeout(1)).printResults(mockDHT,
				job.currentProcedure().dataInputDomain().toString());

	}

	@Test
	public void testTrySubmitTasks() throws Exception {
		// Also tests addTaskFuture and createTaskExecutionRunnable
		Method tryExecuteProcedure = JobCalculationMessageConsumer.class.getDeclaredMethod("trySubmitTasks",
				Procedure.class);
		tryExecuteProcedure.setAccessible(true);

		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 2, false, false);
		JobProcedureDomain in = JobProcedureDomain.create(job.id(), 0, "E1",
				StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain out = JobProcedureDomain.create(job.id(), 0, "E1",
				WordCountMapper.class.getSimpleName(), 1);
		job.currentProcedure().nrOfSameResultHash(0);
		job.currentProcedure().nrOfSameResultHashForTasks(0);
		job.incrementProcedureIndex(); // Currently: second procedure (Wordcount mapper)
		job.currentProcedure().dataInputDomain(in);
		job.currentProcedure().addOutputDomain(out);
		Task helloTask = Task.create("hello", calculationExecutor.id());
		job.currentProcedure().addTask(helloTask);
		Field futuresField = JobCalculationMessageConsumer.class.getDeclaredField("futures");
		futuresField.setAccessible(true);
		Map<String, ListMultimap<Task, Future<?>>> futures = (Map<String, ListMultimap<Task, Future<?>>>) futuresField
				.get(calculationMsgConsumer);
		assertEquals(0, futures.size());
		assertEquals(0, futures.keySet().size());
		assertEquals(false, futures.containsKey(job.currentProcedure().dataInputDomain().toString()));
		tryExecuteProcedure.invoke(calculationMsgConsumer, job.currentProcedure());
		assertEquals(1, futures.size());
		assertEquals(1, futures.keySet().size());
		assertEquals(true, futures.containsKey(job.currentProcedure().dataInputDomain().toString()));
		assertEquals(1, futures.get(job.currentProcedure().dataInputDomain().toString()).keySet().size());
		assertEquals(true,
				futures.get(job.currentProcedure().dataInputDomain().toString()).containsKey(helloTask));
	}

	@Ignore
	public void testCancelTaskExecution() throws Exception {

		// ===========================================================================================================================================
		//
		// ===========================================================================================================================================
		Field futuresField = JobCalculationMessageConsumer.class.getDeclaredField("futures");
		futuresField.setAccessible(true);

		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 2, false, false);
		JobProcedureDomain in = JobProcedureDomain.create(job.id(), 0, "E1",
				StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain out = JobProcedureDomain.create(job.id(), 0, "E1",
				WordCountMapper.class.getSimpleName(), 1);
		job.currentProcedure().nrOfSameResultHash(0);
		job.currentProcedure().nrOfSameResultHashForTasks(0);
		job.incrementProcedureIndex(); // Currently: second procedure (Wordcount mapper)
		job.currentProcedure().dataInputDomain(in);
		job.currentProcedure().addOutputDomain(out);
		Task task1 = Task.create("hello", calculationExecutor.id());
		Task task2 = Task.create("world", calculationExecutor.id());
		job.currentProcedure().addTask(task1);
		job.currentProcedure().addTask(task2);

		// Next submits the tasks
		Method trySubmitTasks = JobCalculationMessageConsumer.class.getDeclaredMethod("trySubmitTasks",
				Procedure.class);
		trySubmitTasks.setAccessible(true);
		trySubmitTasks.invoke(calculationMsgConsumer, job.currentProcedure());
		Map<String, ListMultimap<Task, Future<?>>> futures = (Map<String, ListMultimap<Task, Future<?>>>) futuresField
				.get(calculationMsgConsumer);

		assertEquals(4, futures.get(job.currentProcedure().dataInputDomain().toString()).size());
		assertEquals(2, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).size());
		assertEquals(2, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).size());
		// assertEquals(false,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).get(0).isCancelled());
		// assertEquals(false,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).get(1).isCancelled());
		// assertEquals(false,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).get(0).isCancelled());
		// assertEquals(false,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).get(1).isCancelled());

		calculationMsgConsumer.cancelTaskExecution(job.currentProcedure().dataInputDomain().toString(),
				task1);
		//// futures = (Map<String, ListMultimap<Task, Future<?>>>) futuresField.get(calculationMsgConsumer);
		assertEquals(2, futures.get(job.currentProcedure().dataInputDomain().toString()).size());
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).size());
		assertEquals(2, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).size());
		// assertEquals(true,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).get(0).isCancelled());
		// assertEquals(true,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).get(1).isCancelled());
		// assertEquals(false,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).get(0).isCancelled());
		// assertEquals(false,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).get(1).isCancelled());
		//
		calculationMsgConsumer.cancelTaskExecution(job.currentProcedure().dataInputDomain().toString(),
				task2);
		//// futures = (Map<String, ListMultimap<Task, Future<?>>>) futuresField.get(calculationMsgConsumer);
		//
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).size());
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).size());
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).size());
		// assertEquals(true,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).get(0).isCancelled());
		// assertEquals(true,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).get(1).isCancelled());
		// assertEquals(true,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).get(0).isCancelled());
		// assertEquals(true,
		// futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).get(1).isCancelled());
		futures.clear();
	}

	@Ignore
	public void testCancelProcedure() throws Exception {
		Field futuresField = JobCalculationMessageConsumer.class.getDeclaredField("futures");
		futuresField.setAccessible(true);

		Job job = Job.create("S1").addSucceedingProcedure(WordCountMapper.create(), null, 1, 2, false, false);
		JobProcedureDomain in = JobProcedureDomain.create(job.id(), 0, "E1",
				StartProcedure.class.getSimpleName(), 0);
		JobProcedureDomain out = JobProcedureDomain.create(job.id(), 0, "E1",
				WordCountMapper.class.getSimpleName(), 1);
		job.currentProcedure().nrOfSameResultHash(0);
		job.currentProcedure().nrOfSameResultHashForTasks(0);
		job.incrementProcedureIndex(); // Currently: second procedure (Wordcount mapper)
		job.currentProcedure().dataInputDomain(in);
		job.currentProcedure().addOutputDomain(out);
		Task task1 = Task.create("hello", calculationExecutor.id());
		Task task2 = Task.create("world", calculationExecutor.id());
		job.currentProcedure().addTask(task1);
		job.currentProcedure().addTask(task2);

		// Next submits the tasks
		Method trySubmitTasks = JobCalculationMessageConsumer.class.getDeclaredMethod("trySubmitTasks",
				Procedure.class);
		trySubmitTasks.setAccessible(true);
		trySubmitTasks.invoke(calculationMsgConsumer, job.currentProcedure());
		Map<String, ListMultimap<Task, Future<?>>> futures = (Map<String, ListMultimap<Task, Future<?>>>) futuresField
				.get(calculationMsgConsumer);
		assertEquals(4, futures.get(job.currentProcedure().dataInputDomain().toString()).size());
		assertEquals(2, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).size());
		assertEquals(2, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).size());
		calculationMsgConsumer.cancelProcedureExecution(job.currentProcedure().dataInputDomain().toString());
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).size());
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task1).size());
		assertEquals(0, futures.get(job.currentProcedure().dataInputDomain().toString()).get(task2).size());
		futures.clear();
	}

}
