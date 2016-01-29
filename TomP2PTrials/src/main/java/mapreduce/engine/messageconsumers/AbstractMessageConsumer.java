package mapreduce.engine.messageconsumers;

import mapreduce.engine.executors.IExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.IDHTConnectionProvider;

public class AbstractMessageConsumer implements IMessageConsumer {

	protected IExecutor executor;
	protected IDHTConnectionProvider dhtConnectionProvider;

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
	}

	@Override
	public IExecutor executor() {
		return this.executor;
	}

	@Override
	public IMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public IMessageConsumer executor(IExecutor executor) {
		this.executor = executor;
		return this;
	}

}
