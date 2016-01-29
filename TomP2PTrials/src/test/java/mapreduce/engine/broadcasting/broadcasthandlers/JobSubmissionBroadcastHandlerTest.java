package mapreduce.engine.broadcasting.broadcasthandlers;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;

public class JobSubmissionBroadcastHandlerTest {
	private static JobSubmissionBroadcastHandler broadcastHandler;
	private static JobSubmissionMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUp() {

		messageConsumer = Mockito.mock(JobSubmissionMessageConsumer.class);
		JobSubmissionExecutor executor = Mockito.mock(JobSubmissionExecutor.class);
		Mockito.when(executor.id()).thenReturn("Executor");
		Mockito.when(messageConsumer.executor()).thenReturn(executor);

		broadcastHandler = JobSubmissionBroadcastHandler.create(1);
		broadcastHandler.messageConsumer(messageConsumer);
	}

	@Test
	public void testEvaluateReceivedMessage() {
		Job job = Mockito.mock(Job.class);
		Mockito.when(job.id()).thenReturn("J1");
		Mockito.when(job.isFinished()).thenReturn(false);

		JobProcedureDomain in = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(in.jobId()).thenReturn("J1");
		JobProcedureDomain out = Mockito.mock(JobProcedureDomain.class);
		IBCMessage bcMessage = Mockito.mock(IBCMessage.class);
		Mockito.when(bcMessage.inputDomain()).thenReturn(in);
		Mockito.when(bcMessage.outputDomain()).thenReturn(out);

		broadcastHandler.evaluateReceivedMessage(null);
		Mockito.verify(bcMessage, Mockito.times(0)).execute(job, messageConsumer);
	}

	@Test
	public void testProcessMessage() {
		// ========================================================================================================================================
		// an incoming message should only be forwarded in case it is a COMPLETED_PROCEDURE message for a job
		// that was submitted by this submitter,
		// and only if the job is already finished as else it is not of relevance for retrieval yet.
		// ========================================================================================================================================

		Job job = Mockito.mock(Job.class);
		Mockito.when(job.isFinished()).thenReturn(false);
		IBCMessage bcMessage = Mockito.mock(IBCMessage.class);
		JobProcedureDomain inputDomain = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(inputDomain.isJobFinished()).thenReturn(false);
		Mockito.when(bcMessage.inputDomain()).thenReturn(inputDomain);

		// Only way to check if it entered the else part is to check if bcMessage.execute(job, msgConsumer)
		// was not invoked
		broadcastHandler.processMessage(bcMessage, job);
		Mockito.verify(bcMessage, Mockito.times(0)).execute(job, messageConsumer);

		// Now the job is finished
		Mockito.when(inputDomain.isJobFinished()).thenReturn(true);
		broadcastHandler.processMessage(bcMessage, job);
		Mockito.verify(bcMessage, Mockito.times(1)).execute(job, messageConsumer);

	}

}
