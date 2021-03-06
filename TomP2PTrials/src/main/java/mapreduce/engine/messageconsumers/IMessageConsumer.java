package mapreduce.engine.messageconsumers;

import java.util.List;

import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.IDHTConnectionProvider;

public interface IMessageConsumer {

	public void handleCompletedTask(Job job, List<ExecutorTaskDomain> outputDomains, JobProcedureDomain inputDomain);

	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain);

	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

//	public IExecutor executor();

//	public IMessageConsumer executor(IExecutor executor);

	public void cancelJob(Job job);
	
	public void shutdown();

//	public JobSubmissionExecutor executor();

}
