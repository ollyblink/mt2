package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.broadcasthandlers.AbstractMapReduceBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.jobs.Job;

public abstract class AbstractTimeout implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(AbstractTimeout.class);

	protected volatile AbstractMapReduceBroadcastHandler broadcastHandler;
	protected volatile Job job;
	protected volatile long timeToLive;
	protected volatile long retrievalTimestamp;
	protected volatile IBCMessage bcMessage;
 

	public AbstractTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp,
			IBCMessage bcMessage, long timeToLive) {
		this.broadcastHandler = broadcastHandler;
		this.job = job;
		this.timeToLive = timeToLive;
		retrievalTimestamp(currentTimestamp, bcMessage);

	}

	public AbstractTimeout retrievalTimestamp(long retrievalTimestamp, IBCMessage bcMessage) {
		logger.info("retrievalTimestamp:: updated timeout for job " + job);
		this.retrievalTimestamp = retrievalTimestamp;
		this.bcMessage = bcMessage;
		return this;
	}

	protected void sleep() {
		while ((System.currentTimeMillis() - retrievalTimestamp) < timeToLive) {
			logger.info("sleep:: sleeping for " + timeToLive + " ms");
			try {
				Thread.sleep(timeToLive);
			} catch (InterruptedException e) {
				logger.warn("Exception caught", e);
			}
		}
	}

	public static AbstractTimeout create(AbstractMapReduceBroadcastHandler broadcastHandler, Job job,
			long retrievalTimestamp, IBCMessage bcMessage) {
		return (broadcastHandler instanceof JobSubmissionBroadcastHandler
				? new JobSubmissionTimeout((JobSubmissionBroadcastHandler) broadcastHandler, job,
						retrievalTimestamp, bcMessage, job.submitterTimeToLive())
				: new JobCalculationTimeout((JobCalculationBroadcastHandler) broadcastHandler, job,
						retrievalTimestamp, bcMessage, job.calculatorTimeToLive()));
	}

};