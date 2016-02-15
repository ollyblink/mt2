package mapreduce.engine.messageconsumers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.IDHTConnectionProvider;

public class JobSubmissionMessageConsumer extends AbstractMessageConsumer {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionMessageConsumer.class);

	private JobSubmissionMessageConsumer() {

	}

	public static JobSubmissionMessageConsumer create() {
		return new JobSubmissionMessageConsumer();
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		collect(job, outputDomain, inputDomain);
	}

	@Override
	public void handleCompletedTask(Job job, List<ExecutorTaskDomain> outputDomains, JobProcedureDomain inputDomain) {
		collect(job, outputDomains.get(0).jobProcedureDomain(), inputDomain);
	}

	private void collect(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		if (job == null || outputDomain == null || inputDomain == null || outputDomain.procedureSimpleName() == null || !inputDomain.isJobFinished()) {
			return;
		}
		logger.info("Trying to collect data from " + outputDomain);

		if (job.jobSubmitterID().equals(executor.id()) && executor().submittedJob(job) && !executor().jobIsRetrieved(job) && inputDomain.isJobFinished()) {
			// if (outputDomain.procedureSimpleName().equals(EndProcedure.class.getSimpleName())) {
			logger.info("Job is finished. Final data location domain: " + inputDomain);
			executor().retrieveAndStoreDataOfFinishedJob(outputDomain)  ;
			// }
		}
	}

//	@Override
//	public JobSubmissionExecutor executor() {
//		return (JobSubmissionExecutor) super.executor();
//	}

	@Override
	public JobSubmissionMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		return (JobSubmissionMessageConsumer) super.dhtConnectionProvider(dhtConnectionProvider);
	}

//	@Override
//	public JobSubmissionMessageConsumer executor(IExecutor executor) {
//		return (JobSubmissionMessageConsumer) super.executor(executor);
//	}

//	@Override
//	public void cancelExecution(Job job) {
//		// TODO Auto-generated method stub
//		
//	}
}
