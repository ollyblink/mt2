package mapreduce.engine.broadcasting.broadcasthandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.execution.jobs.Job;

public class JobSubmissionTimeout extends AbstractTimeout {
	private static Logger logger = LoggerFactory.getLogger(JobSubmissionTimeout.class);

	public JobSubmissionTimeout(JobSubmissionBroadcastHandler broadcastHandler, Job job,
			long retrievalTimestamp, IBCMessage bcMessage, long timeToLive, boolean guessTimeout, double fraction) {
		super(broadcastHandler, job, retrievalTimestamp, bcMessage, timeToLive, guessTimeout, fraction);
	}

	@Override
	public void run() {
		sleep();
		logger.info("run:: try resubmitting job " + job);
//		if (job.incrementSubmissionCounter() < job.maxNrOfSubmissionTrials()) {
//			logger.info("run:: after(job.incrementSubmissionCounter() < job.maxNrOfSubmissionTrials())");
//			((JobSubmissionExecutor) broadcastHandler.messageConsumer().executor()).submit(job);
//
//		} else {
//			logger.info("run::job submission aborted. Job: " + job);
//		}
	}

}
