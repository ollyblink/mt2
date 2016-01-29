package mapreduce.engine.broadcasting.broadcasthandlers.timeouts;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.timeout.AbstractTimeout;
import mapreduce.engine.broadcasting.broadcasthandlers.timeout.JobCalculationTimeout;
import mapreduce.engine.broadcasting.broadcasthandlers.timeout.JobSubmissionTimeout;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.storage.IDHTConnectionProvider;

public class TimeoutTests {

	private static final long TIME_TO_LIVE = 40l;
	private static final long SECOND_SLEEP = 80l;
	private static final long FIRST_SLEEP = 20l;
	private JobProcedureDomain oldID;
	private Procedure procedure;
	private Job job;
	private JobCalculationExecutor calculationExecutor;
	private JobCalculationMessageConsumer calculationMsgConsumer;
	private JobCalculationBroadcastHandler calculationBroadcastHandler;
	private IDHTConnectionProvider mockDHT;
	private JobProcedureDomain inputDomain;
	private CompletedBCMessage bcMessage;
	private long currentTimestamp;
	private long timeToLive;
	private CompletedBCMessage mockMsg;
	private JobSubmissionExecutor submissionExecutor;
	private JobSubmissionMessageConsumer submissionMsgConsumer;
	private JobSubmissionBroadcastHandler submissionBroadcastHandler;

	@Before
	public void before() {
		currentTimestamp = System.currentTimeMillis();

		timeToLive = TIME_TO_LIVE;

		// Procedure
		oldID = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(oldID.procedureIndex()).thenReturn(-1);
		Mockito.when(oldID.expectedNrOfFiles()).thenReturn(1);

		procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.tasksSize()).thenReturn(1);
		Mockito.when(procedure.dataInputDomain()).thenReturn(oldID);
		// Job
		job = Mockito.mock(Job.class);
		Mockito.when(job.id()).thenReturn("J1");
		Mockito.when(job.currentProcedure()).thenReturn(procedure);
		// Next message
		mockMsg = Mockito.mock(CompletedBCMessage.class);
		// DHTConnectionProvider
		mockDHT = Mockito.mock(IDHTConnectionProvider.class);
		// Calculation Executor
		calculationExecutor = Mockito.mock(JobCalculationExecutor.class);
		Mockito.when(calculationExecutor.id()).thenReturn("E1");
		Mockito.when(calculationExecutor.tryCompletingProcedure(procedure)).thenReturn(mockMsg);
		// Calculation MessageConsumer
		calculationMsgConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		Mockito.when(calculationMsgConsumer.executor()).thenReturn(calculationExecutor);
		// Calculation BCHandler
		calculationBroadcastHandler = Mockito.mock(JobCalculationBroadcastHandler.class);
		Mockito.when(calculationBroadcastHandler.executorId()).thenReturn("E1");
		Mockito.when(calculationBroadcastHandler.messageConsumer()).thenReturn(calculationMsgConsumer);
		Mockito.when(calculationBroadcastHandler.getJob(job.id())).thenReturn(job);
		Mockito.when(calculationBroadcastHandler.dhtConnectionProvider()).thenReturn(mockDHT);

		// Submission Executor
		submissionExecutor = Mockito.mock(JobSubmissionExecutor.class);
		Mockito.when(submissionExecutor.id()).thenReturn("E1");
		// Submission MessageConsumer
		submissionMsgConsumer = Mockito.mock(JobSubmissionMessageConsumer.class);
		Mockito.when(submissionMsgConsumer.executor()).thenReturn(submissionExecutor);
		// Submission BCHandler
		submissionBroadcastHandler = Mockito.mock(JobSubmissionBroadcastHandler.class);
		Mockito.when(submissionBroadcastHandler.executorId()).thenReturn("E1");
		Mockito.when(submissionBroadcastHandler.messageConsumer()).thenReturn(submissionMsgConsumer);
		Mockito.when(submissionBroadcastHandler.getJob(job.id())).thenReturn(job);
		Mockito.when(submissionBroadcastHandler.dhtConnectionProvider()).thenReturn(mockDHT);

		// Input domain
		inputDomain = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(inputDomain.expectedNrOfFiles()).thenReturn(2);
		// BCMessage
		bcMessage = Mockito.mock(CompletedBCMessage.class);
	}

	@Test
	public void testJobCalculationTimeoutUpdatingExpectedNrOfTasks() throws InterruptedException {
		Mockito.when(inputDomain.procedureIndex()).thenReturn(-1);
		Mockito.when(bcMessage.inputDomain()).thenReturn(inputDomain);

		// Actual timeout
		JobCalculationTimeout timeout = new JobCalculationTimeout(calculationBroadcastHandler, job,
				currentTimestamp, bcMessage, timeToLive);
		Thread t = new Thread(timeout);
		t.start();
		Thread.sleep(FIRST_SLEEP);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		Thread.sleep(SECOND_SLEEP);
		Mockito.verify(job, Mockito.times(1)).currentProcedure();
		// Mockito.verify(procedure, Mockito.times(1)).tasks();
		Mockito.verify(procedure, Mockito.times(1)).dataInputDomain();
		Mockito.verify(procedure, Mockito.times(1)).tasksSize();
		Mockito.verify(calculationBroadcastHandler, Mockito.times(1)).messageConsumer();
		Mockito.verify(calculationBroadcastHandler, Mockito.times(1)).dhtConnectionProvider();
		Mockito.verify(calculationBroadcastHandler, Mockito.times(1)).processMessage(mockMsg, job);
		Mockito.verify(calculationMsgConsumer, Mockito.times(1)).executor();
		Mockito.verify(calculationExecutor, Mockito.times(1)).tryCompletingProcedure(procedure);
		Mockito.verify(inputDomain, Mockito.times(1)).expectedNrOfFiles();
		Mockito.verify(bcMessage, Mockito.times(1)).inputDomain();
	}

	@Test
	public void testJobCalculationTimeoutInputdomainNullTimeout() throws InterruptedException {
		Mockito.when(inputDomain.procedureIndex()).thenReturn(-1);
		Mockito.when(bcMessage.inputDomain()).thenReturn(null);
		// Actual timeout
		JobCalculationTimeout timeout = new JobCalculationTimeout(calculationBroadcastHandler, job,
				currentTimestamp, bcMessage, timeToLive);
		Thread t = new Thread(timeout);
		t.start();
		Thread.sleep(FIRST_SLEEP);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		Thread.sleep(SECOND_SLEEP);
		Mockito.verify(calculationBroadcastHandler, Mockito.times(1)).abortJobExecution(job);
	}

	@Test
	public void testJobCalculationTimeoutInputdomainNotStartProcedureTimeout() throws InterruptedException {
		Mockito.when(inputDomain.procedureIndex()).thenReturn(0);
		Mockito.when(bcMessage.inputDomain()).thenReturn(inputDomain);
		// Actual timeout
		JobCalculationTimeout timeout = new JobCalculationTimeout(calculationBroadcastHandler, job,
				currentTimestamp, bcMessage, timeToLive);
		Thread t = new Thread(timeout);
		t.start();
		Thread.sleep(FIRST_SLEEP);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		Thread.sleep(SECOND_SLEEP);
		Mockito.verify(calculationBroadcastHandler, Mockito.times(1)).abortJobExecution(job);
	}

	@Test
	public void testJobSubmissionTimeoutResubmittingJob() throws Exception {
		submit(2, 1);
	}

	@Test
	public void testJobSubmissionTimeoutNOResubmittingJob() throws Exception {
		submit(1, 0);
	}

	private void submit(int maxNrOfSubmissionTrials, int invoked)
			throws NoSuchFieldException, InterruptedException, IllegalAccessException {
		// Actual timeout
		Mockito.when(job.maxNrOfSubmissionTrials()).thenReturn(maxNrOfSubmissionTrials);
		Mockito.when(job.incrementSubmissionCounter()).thenReturn(1);
		Field retrievalTimestampField = AbstractTimeout.class.getDeclaredField("retrievalTimestamp");
		retrievalTimestampField.setAccessible(true);

		JobSubmissionTimeout timeout = new JobSubmissionTimeout(submissionBroadcastHandler, job,
				currentTimestamp, bcMessage, timeToLive);
		Thread t = new Thread(timeout);
		t.start();
		Thread.sleep(FIRST_SLEEP);

		long retrievalTimestamp = (long) retrievalTimestampField.get(timeout);
		long diff = System.currentTimeMillis() - retrievalTimestamp;
		System.err.println("diff: " + diff + ", After sleep");

		assertEquals(true, diff < timeToLive);
		timeout.retrievalTimestamp(System.currentTimeMillis(), bcMessage);
		retrievalTimestamp = (long) retrievalTimestampField.get(timeout);
		diff = System.currentTimeMillis() - retrievalTimestamp;
		assertEquals(true, diff < timeToLive);
		Thread.sleep(SECOND_SLEEP);
		System.err.println("diff: " + diff + ", After sleep");
		diff = System.currentTimeMillis() - retrievalTimestamp;

		assertEquals(true, diff >= timeToLive);

		System.err.println("diff: " + diff + ", After sleep");
		Mockito.verify(job, Mockito.times(1)).maxNrOfSubmissionTrials();
		Mockito.verify(job, Mockito.times(1)).incrementSubmissionCounter();
		Mockito.verify(submissionExecutor, Mockito.times(invoked)).submit(job);
	}
}
