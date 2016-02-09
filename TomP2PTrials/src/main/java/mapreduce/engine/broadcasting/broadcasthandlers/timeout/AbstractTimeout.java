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
	/** used to execute specific actions once the time runs out */
	protected volatile AbstractMapReduceBroadcastHandler broadcastHandler;
	protected volatile Job job;
	/** ================ */

	/** How long the timeout should wait */
	protected volatile long timeToLive;
	/** When the last message was received */
	protected volatile long retrievalTimestamp = 0;
	/** Last received message */
	protected volatile IBCMessage bcMessage;

	/** Specifies if a dynamic timeout should be guessed from the difference between received messages */
	private volatile boolean guessTimeout;
	/**
	 * Specifies the additional time of the guessed timeout to be added, such that it takes into account possible delays in further reception of messages (timeToLive + (timeToLive*fraction)). Per
	 * default, this value is set to 1, such that the time to live is always twice the time difference between two received messages
	 */
	private volatile double fraction = 1.0;
	/** Only used in case of true guessTimeout to calculate the difference in time between received messages */
	protected volatile long before;
	/** As the timeout is always the maximum timeout, this value specifies the maximum received time between two messages (keep fraction high enough to avoid too early timeout */
	protected volatile long max = Long.MIN_VALUE;

	public AbstractTimeout(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp, IBCMessage bcMessage, long timeToLive, boolean guessTimeout, double fraction) {
		this.broadcastHandler = broadcastHandler;
		this.job = job;
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
		if (guessTimeout) {
			if (before > 0) { // was not the first retrieved
				long diff = 0;
				diff = this.retrievalTimestamp - before;
				if (max < diff) {
					max = diff;
				}
				this.timeToLive = (long) (max + (max * fraction));
				logger.info("Diff: " + diff + ", max: " + max + ", timetolive: " + timeToLive);

			}
		}
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

	public static AbstractTimeout create(AbstractMapReduceBroadcastHandler broadcastHandler, Job job, long currentTimestamp, IBCMessage bcMessage) {
		return (broadcastHandler instanceof JobSubmissionBroadcastHandler
				? new JobSubmissionTimeout((JobSubmissionBroadcastHandler) broadcastHandler, job, currentTimestamp, bcMessage, job.submitterTimeToLive(), job.submitterGuessTimeout(),
						job.submitterTimeoutFraction())
				: new JobCalculationTimeout((JobCalculationBroadcastHandler) broadcastHandler, job, currentTimestamp, bcMessage, job.calculatorTimeToLive(), job.calculatorGuessTimeout(),
						job.calculatorTimeoutFraction()));
	}

};