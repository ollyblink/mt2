package mapreduce.engine.broadcasting.broadcasthandlers;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileUtils;

public class JobCalculationBroadcastHandlerTest {
	private Random random = new Random();
	private JobCalculationBroadcastHandler broadcastHandler;
	private Job job;
	private JobCalculationMessageConsumer messageConsumer;
	private IDHTConnectionProvider dhtConnectionProvider;

	@Before
	public void setUp() {

		String jsMapper = FileUtils.INSTANCE.readLines(System.getProperty("user.dir") + "/src/main/java/mapreduce/execution/procedures/wordcountmapper.js");
		String jsReducer = FileUtils.INSTANCE.readLines(System.getProperty("user.dir") + "/src/main/java/mapreduce/execution/procedures/wordcountreducer.js");

		job = Job.create("Submitter").addSucceedingProcedure(jsMapper, jsReducer).addSucceedingProcedure(jsReducer);

		messageConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		// JobCalculationExecutor executor = Mockito.mock(JobCalculationExecutor.class);
		// Mockito.when(Job).thenReturn("Executor");
		// Mockito.when(messageConsumer.executor()).thenReturn(executor);

		broadcastHandler = JobCalculationBroadcastHandler.create(1);
		broadcastHandler.messageConsumer(messageConsumer);
		dhtConnectionProvider = TestUtils.getTestConnectionProvider(broadcastHandler);
		broadcastHandler.dhtConnectionProvider(dhtConnectionProvider);
	}

	@After
	public void tD() {
		broadcastHandler.shutdown();
		dhtConnectionProvider.shutdown();
	}

	@Test
	public void testJobIsRetrievedOnTASKMessage() throws InterruptedException {
		// Test should make sure that in any case, a job will be tried to gather in case of both Procedure and Task complete messages.

		broadcastHandler.dhtConnectionProvider(dhtConnectionProvider);

		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();

		// Test for completed procedure
		CompletedTaskBCMessage msg = Mockito.mock(CompletedTaskBCMessage.class);
		Mockito.when(msg.status()).thenReturn(BCMessageStatus.COMPLETED_TASK);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1, 0));
		// List list = Mockito.mock(List.class);
		// Mockito.when(msg.allExecutorTaskDomains()).thenReturn(list);
		JobProcedureDomain outputDomain = JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", StartProcedure.class.getSimpleName(), 0, 0);
		// Mockito.when(list.get(0)).thenReturn(outputDomain);
		Mockito.when(msg.outputDomain()).thenReturn(outputDomain);
		broadcastHandler.jobFuturesFor.clear();

		assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		broadcastHandler.evaluateReceivedMessage(msg);
		Thread.sleep(2000);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.getJob(job.id()) != null);
		Job bcHandlerJob = broadcastHandler.jobFutures().keySet().iterator().next();
		assertEquals(0, bcHandlerJob.submissionCount());
		assertEquals(1, broadcastHandler.jobFutures().keySet().size());
		System.err.println(broadcastHandler.jobFutures().keySet());
		broadcastHandler.jobFuturesFor.clear();

	}

	@Test
	public void testJobIsRetrievedOnProcedureMessage() throws InterruptedException {
		// Test should make sure that in any case, a job will be tried to gather in case of both Procedure and Task complete messages.

		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();

		// Test for completed procedure
		CompletedProcedureBCMessage msg = Mockito.mock(CompletedProcedureBCMessage.class);
		Mockito.when(msg.status()).thenReturn(BCMessageStatus.COMPLETED_PROCEDURE);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1, 0));
		Mockito.when(msg.outputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", StartProcedure.class.getSimpleName(), 0, 0));
		broadcastHandler.jobFuturesFor.clear();
		assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		broadcastHandler.evaluateReceivedMessage(msg);
		Thread.sleep(2000);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.getJob(job.id()) != null);
		Job bcHandlerJob = broadcastHandler.jobFutures().keySet().iterator().next();
		assertEquals(0, bcHandlerJob.submissionCount());
		assertEquals(1, broadcastHandler.jobFutures().keySet().size());
		System.err.println(broadcastHandler.jobFutures().keySet());
		broadcastHandler.jobFuturesFor.clear();

	}

	@Test
	public void testEvaluateReceivedMessage() throws Exception {
 
 
		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();

		CompletedProcedureBCMessage msg = Mockito.mock(CompletedProcedureBCMessage.class);
		Mockito.when(msg.status()).thenReturn(BCMessageStatus.COMPLETED_PROCEDURE);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1, 0));
		Mockito.when(msg.outputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", StartProcedure.class.getSimpleName(), 0, 0));

		assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		broadcastHandler.evaluateReceivedMessage(msg);
		Thread.sleep(2000);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.getJob(job.id()) != null);
		Job bcHandlerJob = broadcastHandler.jobFutures().keySet().iterator().next();
		assertEquals(0, bcHandlerJob.submissionCount());
		assertEquals(1, broadcastHandler.jobFutures().keySet().size());
		System.err.println(broadcastHandler.jobFutures().keySet());
		ListMultimap<Job, Future<?>> jobFutures = broadcastHandler.jobFutures();
		for (Future<?> f : jobFutures.values()) {
			assertEquals(true, f.isDone());
		}
		// check if all procedure Java Scripts were converted to IExecutables
		assertEquals(true, job.procedure(1).executable() instanceof String);
		assertEquals(true, job.procedure(2).executable() instanceof String);
		assertEquals(true, bcHandlerJob.procedure(1).executable() instanceof IExecutable);
		assertEquals(true, bcHandlerJob.procedure(2).executable() instanceof IExecutable);

		// Check that job is updated in broadcasthandler
		job.incrementSubmissionCounter();
		msg = CompletedProcedureBCMessage.create(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1, 0),
				JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", StartProcedure.class.getSimpleName(), 0, 0));
		broadcastHandler.evaluateReceivedMessage(msg);
		Thread.sleep(1000);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.getJob(job.id()) != null);
		assertEquals(1, bcHandlerJob.submissionCount());
		assertEquals(1, broadcastHandler.jobFutures().keySet().size());
		System.err.println(broadcastHandler.jobFutures().keySet());
		jobFutures = broadcastHandler.jobFutures();
		for (Future<?> f : jobFutures.values()) {
			assertEquals(true, f.isDone());
		}
		dhtConnectionProvider.shutdown();
	}

	@Test
	public void testProcessMessage() throws InterruptedException {
		CompletedProcedureBCMessage msg = Mockito.mock(CompletedProcedureBCMessage.class);
		Mockito.when(msg.status()).thenReturn(BCMessageStatus.COMPLETED_PROCEDURE);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1, 0));
		Mockito.when(msg.outputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", StartProcedure.class.getSimpleName(), 0, 0));

		broadcastHandler.jobFutures().clear();
		broadcastHandler.processMessage(msg, job);
		Thread.sleep(100);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.getJob(job.id()) != null);
		assertEquals(1, broadcastHandler.jobFutures().keySet().size());

		// Mockito.verify(msg, Mockito.times(1)).execute(job, messageConsumer);
		msg = Mockito.mock(CompletedProcedureBCMessage.class);
		Mockito.when(msg.status()).thenReturn(BCMessageStatus.COMPLETED_PROCEDURE);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1, 0));
		Mockito.when(msg.outputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", StartProcedure.class.getSimpleName(), 0, 0));

		broadcastHandler.jobFutures().clear();
		// The next one should try it with a finished job. Nothing should happen and jobFutures should stay
		// empty
		job = Job.create("Submitter");
		job.currentProcedure().nrOfSameResultHash(0)
		// .nrOfSameResultHashForTasks(0)
		;
		broadcastHandler.processMessage(msg, job);

		assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		assertEquals(false, broadcastHandler.getJob(job.id()) != null);
		assertEquals(0, broadcastHandler.jobFutures().keySet().size());
		// Mockito.verify(msg, Mockito.times(0)).execute(job, messageConsumer);
	}
}
