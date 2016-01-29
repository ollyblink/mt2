package mapreduce.engine.broadcasting.messages;

import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;

public class CompletedTaskBCMessageTest {

	@Test
	public void testExecuteCompletedProcedure() {
		JobProcedureDomain in = Mockito.mock(JobProcedureDomain.class);
		JobProcedureDomain out = Mockito.mock(JobProcedureDomain.class);
		IMessageConsumer msgConsumer = Mockito.mock(IMessageConsumer.class);
		Job job = Mockito.mock(Job.class);
		CompletedBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(out, in);
		msg.execute(job, msgConsumer);
		Mockito.verify(msgConsumer, Mockito.times(1)).handleCompletedProcedure(job, out, in);
	}

	@Test
	public void testExecuteCompletedTask() {
		JobProcedureDomain in = Mockito.mock(JobProcedureDomain.class);
		ExecutorTaskDomain out = Mockito.mock(ExecutorTaskDomain.class);
		IMessageConsumer msgConsumer = Mockito.mock(IMessageConsumer.class);
		Job job = Mockito.mock(Job.class);
		CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(out, in);
		msg.execute(job, msgConsumer);
		Mockito.verify(msgConsumer, Mockito.times(1)).handleCompletedTask(job, out, in);
	}
}
