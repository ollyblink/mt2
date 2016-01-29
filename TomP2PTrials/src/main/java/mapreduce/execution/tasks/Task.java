package mapreduce.execution.tasks;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.finishables.AbstractFinishable;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public class Task extends AbstractFinishable implements Serializable, Cloneable {
	private static Logger logger = LoggerFactory.getLogger(Task.class);


	/**
	 * 
	 */
	private static final long serialVersionUID = 4696324648240806323L;
	/** Key of this task to get the values for */
	private final String key;
	/** The executor of this task. */
	private String localExecutorId;
	/**
	 * Set true if this tasks's result keys and values were successfully transferred from executor task domain to executor job procedure domain
	 */
	private volatile boolean isInProcedureDomain = false;
	/** Used in the scheduler to not schedule too many executions of the same task */
	private volatile int activeCount = 0;
	/** Just a counter to be used in the executor task domains */
	private volatile int statusIndex = 0;

	private Task decrementActiveCount() {
		if (this.activeCount > 0) {
			--this.activeCount;
		}
		return this;
	}

	/**
	 * Checks if a task can be executed or not. Important when adding to the executor in @see{JobCalculationMessageConsumer.trySubmitTasks}
	 * 
	 * @param localExecutorId
	 * @return
	 */
	public boolean canBeExecuted() {
		if (needsMultipleDifferentExecutors) {
			// Active count is only increased when the local executor starts execution. As such, when active
			// count is 1, it cannot be executed more.
			// Else, it has to be checked if this executor actually finished. If not: can execute, else it
			// cannot be executed
			if (containsExecutor(this.localExecutorId)) {
				return false;
			} else {
				return activeCount < 1;
			}
		} else { // Doesn't matter, can execute until all tasks are finished also by the same executor
			return currentMaxNrOfSameResultHash() + activeCount < nrOfSameResultHash;
		}
	}

	@Override
	public Task addOutputDomain(IDomain domain) {
		if (!this.outputDomains.contains(domain) && domain.executor().equals(localExecutorId)) {
			decrementActiveCount();
		}
		return (Task) super.addOutputDomain(domain);

	}

	@Override
	public boolean isFinished() {
		boolean isFinished = super.isFinished();
		logger.info("isFinished():: [" + key + "] is finished? (" + isFinished + ")");
		return isFinished;
	}

	public Task incrementActiveCount() {
		if (canBeExecuted()) {
			++this.activeCount;
		}
		return this;
	}

	public Integer activeCount() {
		return activeCount;
	}

	/**
	 * Earlier, this had an actual meaning. Now it's only there to tell apart the executions if the same executor executes the task multiple times
	 * 
	 * @return
	 */
	public int newStatusIndex() {
		return this.statusIndex++;
	}

	private Task() {
		this.key = "";
		this.localExecutorId = "";
	}

	private Task(String key, String localExecutorId) {
		this.key = key;
		this.localExecutorId = localExecutorId;
	}

	public static Task create(String key, String localExecutorId) {
		return new Task(key, localExecutorId);
	}

	public String key() {
		return this.key;
	}

	public boolean isInProcedureDomain() {
		return this.isInProcedureDomain;
	}

	public Task isInProcedureDomain(boolean isInProcedureDomain) {
		this.isInProcedureDomain = isInProcedureDomain;
		return this;
	}

	@Override
	public Number160 resultHash() {
		if (resultOutputDomain == null) {
			checkIfFinished();
		}
		return (resultOutputDomain == null ? null : resultOutputDomain.resultHash());
	}

	public void reset() {
		super.reset();
		outputDomains.clear();
		isInProcedureDomain = false;
		activeCount = 0;
		resultOutputDomain = null;
	}

	@Override
	public Task nrOfSameResultHash(int nrOfSameResultHash) {
		return (Task) super.nrOfSameResultHash(nrOfSameResultHash);
	}

	@Override
	public ExecutorTaskDomain resultOutputDomain() {
		return (ExecutorTaskDomain) super.resultOutputDomain();
	}

	@Override
	public String toString() {
		return "Task [key=" + key + ", isInProcedureDomain=" + isInProcedureDomain + ", outputDomains=" + outputDomains + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Task other = (Task) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	protected Task clone() {

		try {
			Task task = (Task) super.clone();
			task.outputDomains = SyncedCollectionProvider.syncedArrayList();
			for (IDomain e : outputDomains) {
				task.outputDomains.add((IDomain) e.clone());
			}
			task.resultOutputDomain = (IDomain) resultOutputDomain.clone();
			return task;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

}
