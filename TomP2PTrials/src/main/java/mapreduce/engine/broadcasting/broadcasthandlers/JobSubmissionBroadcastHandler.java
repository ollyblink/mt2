package mapreduce.engine.broadcasting.broadcasthandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;

public class JobSubmissionBroadcastHandler extends AbstractMapReduceBroadcastHandler {

	 private static Logger logger = LoggerFactory.getLogger(JobSubmissionBroadcastHandler.class);

	@Override
	public void evaluateReceivedMessage(IBCMessage bcMessage) {
		if (bcMessage == null || bcMessage.inputDomain() == null || bcMessage.inputDomain().jobId() == null) {
			return;
		}
		String jobId = bcMessage.inputDomain().jobId();
		// System.err.println("Job id: " +jobId);
		// System.err.println("((JobSubmissionMessageConsumer) messageConsumer).executor().job(jobId) "+((JobSubmissionMessageConsumer) messageConsumer).executor().job(jobId));
		Job job = ((JobSubmissionMessageConsumer) messageConsumer).executor().job(jobId);

		// Only receive messages for jobs that have been added by this submitter
		if (job.jobSubmitterID().equals(JobSubmissionExecutor.classId)) {
			processMessage(bcMessage, job);
		}

	}

	@Override
	public void processMessage(IBCMessage bcMessage, Job job) {
		if (job == null || bcMessage == null) {
			return;
		}
		logger.info("received message: " + bcMessage);
		if (bcMessage.inputDomain().isJobFinished()) {
			if (bcMessage.status() == BCMessageStatus.COMPLETED_TASK) {
				CompletedTaskBCMessage taskMsg = (CompletedTaskBCMessage) bcMessage;
				messageConsumer.handleCompletedTask(job, taskMsg.allExecutorTaskDomains(), bcMessage.inputDomain());
			} else { // status == BCMessageStatus.COMPLETED_PROCEDURE
				messageConsumer.handleCompletedProcedure(job, (JobProcedureDomain) bcMessage.outputDomain(), bcMessage.inputDomain());
			}
			stopTimeout(job);
			return;
		} else {
			updateTimeout(job, bcMessage);
		}
	}

	/**
	 * 
	 * @return
	 */
	public static JobSubmissionBroadcastHandler create() {
		return new JobSubmissionBroadcastHandler();
	}

	// /**
	// *
	// *
	// * @param nrOfConcurrentlyExecutedBCMessages
	// * number of threads for this thread pool: how many bc messages may be executed at the same time?
	// * @return
	// */
	// public static JobSubmissionBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
	// return new JobSubmissionBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	// }

	// Setter, Getter, Creator, Constructor follow below..
	private JobSubmissionBroadcastHandler() {
		super();
	}

	@Override
	public JobSubmissionBroadcastHandler messageConsumer(IMessageConsumer messageConsumer) {
		return (JobSubmissionBroadcastHandler) super.messageConsumer((JobSubmissionMessageConsumer) messageConsumer);
	}
}
