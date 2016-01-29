package mapreduce.engine.messageconsumers;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;

public class JobSubmissionMessageConsumerTest {

	private JobSubmissionMessageConsumer messageConsumer;
	private JobSubmissionExecutor executor;

	@Before
	public void setUpBeforeTest() throws Exception {
		executor = Mockito.mock(JobSubmissionExecutor.class);
		Mockito.when(executor.id()).thenReturn("S1");
		messageConsumer = JobSubmissionMessageConsumer.create().executor(executor);

	}

	@Test
	public void testCollect() throws Exception {
		// =========================================================================================================================================
		// Only submitted jobs should be recovered by this message consumer, all others should be ignored. This can be checked by verifying that the
		// executors "retrieveDataOfFInishedJob(outputDomain)" method is called
		// =========================================================================================================================================
		Method collectMethod = JobSubmissionMessageConsumer.class.getDeclaredMethod("collect", Job.class, JobProcedureDomain.class,
				JobProcedureDomain.class);
		collectMethod.setAccessible(true);
		Job job = Mockito.mock(Job.class);
		Mockito.when(job.jobSubmitterID()).thenReturn("S1");
		EndProcedure mockEndProc = EndProcedure.create();
		Procedure procedure = Mockito.mock(Procedure.class);
		Mockito.when(procedure.executable()).thenReturn(mockEndProc);
		Mockito.when(job.currentProcedure()).thenReturn(procedure);
		JobProcedureDomain outputDomain = Mockito.mock(JobProcedureDomain.class);
		Mockito.when(outputDomain.procedureSimpleName()).thenReturn(EndProcedure.class.getSimpleName());
		JobProcedureDomain inputDomain = Mockito.mock(JobProcedureDomain.class);

		// First: not same submitter --> ignore
		Job job2 = Mockito.mock(Job.class);
		Mockito.when(job2.isFinished()).thenReturn(true);
		Mockito.when(job2.jobSubmitterID()).thenReturn("S2");
		Mockito.when(executor.submittedJob(job2)).thenReturn(false);
		Mockito.when(executor.jobIsRetrieved(job2)).thenReturn(false);
		collectMethod.invoke(messageConsumer, job2, outputDomain, inputDomain);
		Mockito.verify(executor, Mockito.times(0)).retrieveAndStoreDataOfFinishedJob(outputDomain);

		// Second: same submitter, never submitted (? don't know yet where this may happen, just make it sure it never happens here)
		Mockito.when(executor.submittedJob(job)).thenReturn(false);
		Mockito.when(executor.jobIsRetrieved(job)).thenReturn(false);
		Mockito.when(inputDomain.isJobFinished()).thenReturn(false);
		collectMethod.invoke(messageConsumer, job, outputDomain, inputDomain);
		Mockito.verify(executor, Mockito.times(0)).retrieveAndStoreDataOfFinishedJob(outputDomain);

		// Third: same submitter, submitted, but already retrieved once... (May happen if a message from another finishing executor comes in -->
		// ignore!)
		Mockito.when(executor.submittedJob(job)).thenReturn(true);
		Mockito.when(executor.jobIsRetrieved(job)).thenReturn(true);
		Mockito.when(inputDomain.isJobFinished()).thenReturn(false);
		collectMethod.invoke(messageConsumer, job, outputDomain, inputDomain);
		Mockito.verify(executor, Mockito.times(0)).retrieveAndStoreDataOfFinishedJob(outputDomain);

		// Third: same submitter, submitted, job finished, never retrieved before: retrieval possible!
		Mockito.when(executor.submittedJob(job)).thenReturn(true);
		Mockito.when(executor.jobIsRetrieved(job)).thenReturn(false);
		Mockito.when(outputDomain.isJobFinished()).thenReturn(true);
		Mockito.when(inputDomain.isJobFinished()).thenReturn(true);
		collectMethod.invoke(messageConsumer, job, outputDomain, inputDomain);
		Mockito.verify(executor, Mockito.times(1)).retrieveAndStoreDataOfFinishedJob(outputDomain);
	}
}
