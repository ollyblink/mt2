package mapreduce.engine.broadcasting.broadcasthandlers;

import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.jobs.Job;

public class JobSubmissionBroadcastHandler extends AbstractMapReduceBroadcastHandler {

	// private static Logger logger = LoggerFactory.getLogger(JobSubmissionBroadcastHandler.class);

	@Override
	public void evaluateReceivedMessage(IBCMessage bcMessage) {
		if (bcMessage == null || bcMessage.inputDomain() == null || bcMessage.inputDomain().jobId() == null) {
			return;
		}
		String jobId = bcMessage.inputDomain().jobId();
		Job job = ((JobSubmissionMessageConsumer) messageConsumer).executor().job(jobId);

		// Only receive messages for jobs that have been added by this submitter
		if (job != null && messageConsumer.executor() != null && messageConsumer.executor().id() != null
				&& job.jobSubmitterID() != null
				&& job.jobSubmitterID().equals(messageConsumer.executor().id())) {
			processMessage(bcMessage, job);
		}

	}

	@Override
	public void processMessage(IBCMessage bcMessage, Job job) {
		if (job == null || bcMessage == null) {
			return;
		}
		if (bcMessage.inputDomain().isJobFinished()) {
			bcMessage.execute(job, messageConsumer);
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
		return new JobSubmissionBroadcastHandler(1);
	}

	/**
	 *
	 * 
	 * @param nrOfConcurrentlyExecutedBCMessages
	 *            number of threads for this thread pool: how many bc messages may be executed at the same
	 *            time?
	 * @return
	 */
	public static JobSubmissionBroadcastHandler create(int nrOfConcurrentlyExecutedBCMessages) {
		return new JobSubmissionBroadcastHandler(nrOfConcurrentlyExecutedBCMessages);
	}

	// Setter, Getter, Creator, Constructor follow below..
	private JobSubmissionBroadcastHandler(int nrOfConcurrentlyExecutedBCMessages) {
		super(nrOfConcurrentlyExecutedBCMessages);
	}

	@Override
	public JobSubmissionBroadcastHandler messageConsumer(IMessageConsumer messageConsumer) {
		return (JobSubmissionBroadcastHandler) super.messageConsumer(
				(JobSubmissionMessageConsumer) messageConsumer);
	}
}
