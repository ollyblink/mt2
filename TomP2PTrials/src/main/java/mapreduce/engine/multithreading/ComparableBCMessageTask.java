package mapreduce.engine.multithreading;

import java.util.concurrent.FutureTask;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.execution.jobs.PriorityLevel;

public class ComparableBCMessageTask<T> extends FutureTask<T> implements Comparable<ComparableBCMessageTask<T>> {
	private volatile PriorityLevel jobPriority;
	private volatile Long jobCreationTime;
	private volatile BCMessageStatus messageStatus;
	private volatile Long messageCreationTime;
	private volatile Integer procedureIndex;

	public ComparableBCMessageTask(Runnable runnable, T result, PriorityLevel jobPriority, Long jobCreationTime, Integer procedureIndex,
			BCMessageStatus messageStatus, Long messageCreationTime) {
		super(runnable, result);
		this.jobPriority = jobPriority;
		this.jobCreationTime = jobCreationTime;
		this.procedureIndex = procedureIndex;
		this.messageStatus = messageStatus;
		this.messageCreationTime = messageCreationTime;
	}

	@Override
	public int compareTo(ComparableBCMessageTask<T> o) {
		int result = 0;
		if (jobPriority.equals(o.jobPriority)) {
			if (jobCreationTime.equals(o.jobCreationTime)) {
				if (procedureIndex.equals(o.procedureIndex)) {
					if (messageStatus.equals(o.messageStatus)) {
						if (messageCreationTime.equals(o.messageCreationTime)) {
							result = 0;
						} else {
							result = messageCreationTime.compareTo(o.messageCreationTime);
						}
					} else {
						result = messageStatus.compareTo(o.messageStatus);
					}
				} else {
					result = -procedureIndex.compareTo(o.procedureIndex);
				}
			} else {
				result = jobCreationTime.compareTo(o.jobCreationTime);
			}
		} else {
			result = jobPriority.compareTo(o.jobPriority);
		} 
		return result;
	}

	public PriorityLevel getJobPriority() {
		return jobPriority;
	}

	public void setJobPriority(PriorityLevel jobPriority) {
		this.jobPriority = jobPriority;
	}

	public Long getJobCreationTime() {
		return jobCreationTime;
	}

	public void setJobCreationTime(Long jobCreationTime) {
		this.jobCreationTime = jobCreationTime;
	}

	public BCMessageStatus getMessageStatus() {
		return messageStatus;
	}

	public void setMessageStatus(BCMessageStatus messageStatus) {
		this.messageStatus = messageStatus;
	}

	public Long getMessageCreationTime() {
		return messageCreationTime;
	}

	public void setMessageCreationTime(Long messageCreationTime) {
		this.messageCreationTime = messageCreationTime;
	}

	public Integer getProcedureIndex() {
		return procedureIndex;
	}

	public void setProcedureIndex(Integer procedureIndex) {
		this.procedureIndex = procedureIndex;
	}

	@Override
	public String toString() {
		return "[" + jobPriority + ", " + jobCreationTime + ", " + procedureIndex + ", " + messageStatus + ", " + messageCreationTime + "]";
	}

}

// public ComparableFutureTask(Callable<T> callable, int priority) {
// super(callable);
// }