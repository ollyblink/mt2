package mapreduce.engine.broadcasting.broadcasthandlers.timeout;

import java.io.Serializable;

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
	protected volatile long retrievalTimestamp = 0;
	protected volatile IBCMessage bcMessage;

	protected volatile long before;
	protected volatile long max = Long.MIN_VALUE;

	private volatile double fraction = 1.0;

	private volatile boolean guessTimeout;

	private volatile long initialTimeToLive;

	public AbstractTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp, IBCMessage bcMessage, long timeToLive, boolean guessTimeout, long initialTimeToLive, double fraction) {
		this.broadcastHandler = broadcastHandler;
		this.job = job;
		this.initialTimeToLive = initialTimeToLive;
		this.fraction = fraction;
		this.timeToLive = timeToLive;
		this.guessTimeout = guessTimeout;
		retrievalTimestamp(currentTimestamp, bcMessage);

	}

	public AbstractTimeout retrievalTimestamp(long retrievalTimestamp, IBCMessage bcMessage) {
		logger.info("retrievalTimestamp:: updated timeout for job " + job);
		this.before = this.retrievalTimestamp;
		this.retrievalTimestamp = retrievalTimestamp;
		this.bcMessage = bcMessage;
		return this;
	}

	protected void sleep() {
		if (guessTimeout) {
			while (dynamicTimeToLiveCriterion()) {
				logger.info("sleep:: sleeping for " + timeToLive + " ms");
				try {
					Thread.sleep(timeToLive);
				} catch (InterruptedException e) {
					logger.warn("Exception caught", e);
				}
			}
		} else {
			while (staticTimeToLiveCriterion()) {
				logger.info("sleep:: sleeping for " + timeToLive + " ms");
				try {
					Thread.sleep(timeToLive);
				} catch (InterruptedException e) {
					logger.warn("Exception caught", e);
				}
			}
		}
	}

	private boolean dynamicTimeToLiveCriterion() {
		long diff = 0;
		if (before > 0) { // was not the first retrieved
			diff = retrievalTimestamp - before;
			if (max < diff) {
				max = diff;
			}
			this.timeToLive = (long) (max + (max * fraction));
		}
		return staticTimeToLiveCriterion();
	}

	private boolean staticTimeToLiveCriterion() {
		return (System.currentTimeMillis() - retrievalTimestamp) < timeToLive;
	}

	public static AbstractTimeout create(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp, IBCMessage bcMessage, boolean guessTimeout, long initialTimeToLive, double fraction) {
		return (broadcastHandler instanceof JobSubmissionBroadcastHandler
				? new JobSubmissionTimeout((JobSubmissionBroadcastHandler) broadcastHandler, job, currentTimestamp, bcMessage, job.submitterTimeToLive(), guessTimeout, initialTimeToLive, fraction)
				: new JobCalculationTimeout((JobCalculationBroadcastHandler) broadcastHandler, job, currentTimestamp, bcMessage, job.calculatorTimeToLive(), guessTimeout,initialTimeToLive, fraction));
	}

};