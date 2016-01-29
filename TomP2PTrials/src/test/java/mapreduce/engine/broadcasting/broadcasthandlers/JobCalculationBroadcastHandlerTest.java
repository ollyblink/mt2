package mapreduce.engine.broadcasting.broadcasthandlers;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.executors.JobCalculationExecutor;
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

	@Before
	public void setUp() {

		String jsMapper = FileUtils.INSTANCE.readLines(System.getProperty("user.dir")
				+ "/src/main/java/mapreduce/execution/procedures/wordcountmapper.js");
		String jsReducer = FileUtils.INSTANCE.readLines(System.getProperty("user.dir")
				+ "/src/main/java/mapreduce/execution/procedures/wordcountreducer.js");

		job = Job.create("Submitter").addSucceedingProcedure(jsMapper, jsReducer, 1, 1, false, false)
				.addSucceedingProcedure(jsReducer, null, 1, 1, false, false);

		messageConsumer = Mockito.mock(JobCalculationMessageConsumer.class);
		JobCalculationExecutor executor = Mockito.mock(JobCalculationExecutor.class);
		Mockito.when(executor.id()).thenReturn("Executor");
		Mockito.when(messageConsumer.executor()).thenReturn(executor);

		broadcastHandler = JobCalculationBroadcastHandler.create(1);
		broadcastHandler.messageConsumer(messageConsumer);
	}

	@Test
	public void testEvaluateReceivedMessage() throws Exception {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils
				.getTestConnectionProvider(random.nextInt(50000) + 4000, 1);

		broadcastHandler.dhtConnectionProvider(dhtConnectionProvider);

		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();

		CompletedBCMessage msg = Mockito.mock(CompletedBCMessage.class);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(),
				"Submitter", StartProcedure.class.getSimpleName(), 0));
		Mockito.when(msg.outputDomain()).thenReturn(
				JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1));

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
		msg = CompletedBCMessage.createCompletedProcedureBCMessage(
				JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1),
				JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter",
						StartProcedure.class.getSimpleName(), 0));
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
		CompletedBCMessage msg = Mockito.mock(CompletedBCMessage.class);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(),
				"Submitter", StartProcedure.class.getSimpleName(), 0));
		Mockito.when(msg.outputDomain()).thenReturn(
				JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1));

		broadcastHandler.jobFutures().clear();
		broadcastHandler.processMessage(msg, job);
		Thread.sleep(100);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.getJob(job.id()) != null);
		assertEquals(1, broadcastHandler.jobFutures().keySet().size());
		
		Mockito.verify(msg, Mockito.times(1)).execute(job, messageConsumer);
		msg = Mockito.mock(CompletedBCMessage.class);
		Mockito.when(msg.inputDomain()).thenReturn(JobProcedureDomain.create(job.id(), job.submissionCount(),
				"Submitter", StartProcedure.class.getSimpleName(), 0));
		Mockito.when(msg.outputDomain()).thenReturn(
				JobProcedureDomain.create(job.id(), job.submissionCount(), "Submitter", "INITIAL", -1));

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
		Mockito.verify(msg, Mockito.times(0)).execute(job, messageConsumer);
	}
}
