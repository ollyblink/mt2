package mapreduce.execution.procedures;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.finishables.AbstractFinishable;
import mapreduce.execution.tasks.Task;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

/**
 * 
 * @author Oliver
 *
 */
public class Procedure extends AbstractFinishable implements Serializable, Cloneable {
	private static Logger logger = LoggerFactory.getLogger(Procedure.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1717123684693430690L;
	/** The actual procedure to execute */
	private Object executable;
	/**
	 * Which procedure in the job's procedure list (@link{Job} it is (counted from 0 == StartProcedure to N-1 == EndProcedure)
	 */
	private int procedureIndex;
	/** Tasks this procedure needs to execute */
	private List<Task> tasks;
	/** Location of keys to create the tasks for this procedure */
	private JobProcedureDomain dataInputDomain;
	/**
	 * Used to combine data before it is sent to the dht. "Local" aggregation. Is often the same as the subsequent procedure (e.g. WordCount: Combiner of WordCountMapper would be WordCountReducer as
	 * it locally reduces the words). It is not guaranteed that this always works!
	 */
	private Object combiner;
	/**
	 * How many times should each task be executed and reach the same result hash until it is assumed to be a correct answer?
	 */
	private int nrOfSameResultHashForTasks = 0;
	/** Assert that there are multiple output domains received before a task is finished */
	private boolean needsMultipleDifferentExecutorsForTasks;
	/**
	 * Just to make sure this indeed is the same procedure of the same job (may be another job with the same procedure)
	 */
	private String jobId;
	private int taskPointer = 0;

	private boolean isFinished;
	/** Specifies how many executions need to be undertaken for this procedure. Needed for the JobCalculationExecutor to know when execution finished */
	private int numberOfExecutions;

	/**
	 * Factor used especially in task intensive jobs, where each task is rather small and quick to calculate, to summarise the number of results sent. Reduces number of sent messages over network but
	 * increases the risk of losing results in case a broadcast is not received. Used as a fraction of the total number of task (default is 0.0, meaning that every finished task is sent as a message.
	 * 1.0 means that only one message is sent when all tasks finished)
	 */
	private double taskSummarisationFactor = 0.0;

	private Procedure(Object executable, int procedureIndex) {
		this.executable = executable;
		this.procedureIndex = procedureIndex;
		this.tasks = SyncedCollectionProvider.syncedArrayList();
		this.dataInputDomain = null;
	}

	public static Procedure create(Object executable, int procedureIndex) {
		return new Procedure(executable, procedureIndex);
	}

	public Procedure executable(IExecutable executable) {
		this.executable = executable;
		return this;
	}

	@Override
	public boolean isFinished() {
		if (!isFinished) {
			this.isFinished = super.isFinished();
		}
		logger.info("isFinished():: [" + executable.getClass().getSimpleName() + "] is finished? (" + isFinished + ")");
		return this.isFinished;
	}

	@Override
	// Calculates the result hash as an XOR of all the task's result hash's with Number160.ZERO as a base
	// hash.
	// This method only returns a hash if all tasks finished and produced a hash. In any other case, null
	// is returned finished yet
	public Number160 resultHash() {
		if (dataInputDomain == null || tasks.size() < dataInputDomain.expectedNrOfFiles()) {
			return null;
		} else {
			Number160 resultHash = null;
			synchronized (tasks) {
				for (Task task : tasks) {
					if (resultHash == null) {
						resultHash = Number160.ZERO;
					}
					if (task.isFinished()) {
						Number160 taskResultHash = task.resultHash();
						if (taskResultHash != null) {
							resultHash = resultHash.xor(taskResultHash);
						} else {
							return null; // This result hash has not been set yet although the task is
											// finished --> should never happen
						}
					} else {
						return null; // All tasks have to be finished before this can be called
					}
				}
			}
			return resultHash;
		}
	}

	/**
	 * How many tasks of this procedure have finished (be aware: simply having all tasks finished does not mean that all tasks were already received) --> Does not imply the procedure is completed yet!
	 * This method is exclusively used to inform other executors about the finishing state of this executor. If it should be the case that two executors execute the same procedure on different input
	 * data sets, nrOfFinishedTasks will determine which executor to cancel and which to keep (the idea is to execute only on the same data set to keep results consistent, even if the data may have
	 * been corrupted as there is no way to determine that beforehand.
	 * 
	 * 
	 * @return
	 */
	public int nrOfFinishedAndTransferredTasks() {
		int finishedTasksCounter = 0;
		synchronized (tasks) {
			for (Task task : tasks) {
				if (task.isFinished() && task.isInProcedureDomain()) {
					++finishedTasksCounter;
				}
			}
		}
		return finishedTasksCounter;
	}

	/** Reset the result domains of the tasks, such that this procedure may be executed once more */
	@Override
	public void reset() {
		super.reset();
		this.incrementExecutionNumber();
		this.numberOfExecutions = 0;
		synchronized (tasks) {
			for (Task task : tasks) {
				task.reset();
			}
		}
		this.isFinished = false;
	}

	/**
	 * This method may be used to keep the state of being finished for a procedure while removing everything else that may cause RAM to overflow...
	 */
	public void clear() {
		super.reset();
		tasks.clear();
		this.isFinished = true;
	}

	// SETTER/GETTER
	@Override
	// Convenience for Fluent
	public Procedure nrOfSameResultHash(int nrOfSameResultHash) {
		return (Procedure) super.nrOfSameResultHash(nrOfSameResultHash);
	}

	/**
	 * Used to assign to tasks while creating them
	 * 
	 * @param nrOfSameResultHashForTasks
	 *            see description above
	 * @return
	 */
	public Procedure nrOfSameResultHashForTasks(int nrOfSameResultHashForTasks) {
		this.nrOfSameResultHashForTasks = nrOfSameResultHashForTasks;
		synchronized (tasks) { // If it's set on the go, should update all tasks (hopefully never happens...)
			for (Task task : tasks) {
				task.nrOfSameResultHash(nrOfSameResultHashForTasks);
			}
		}
		return this;
	}

	public int procedureIndex() {
		return this.procedureIndex;
	}

	public Procedure procedureIndex(int procedureIndex) {
		this.procedureIndex = procedureIndex;
		return this;
	}

	public boolean isCompleted() {
		if (tasks.size() == 0) {
			return false; // If tasks's size is 0, something certainly is odd and the procedure is not yet
							// finished
		}
		synchronized (tasks) {
			for (Task task : tasks) {
				if (!task.isFinished() || !task.isInProcedureDomain()) {
					return false;
				}
			}
		}
		return true;
	}

	public Procedure addTask(Task task) {
		synchronized (tasks) {
			if (!this.tasks.contains(task)) {
				task.nrOfSameResultHash(nrOfSameResultHashForTasks);
				task.needsMultipleDifferentExecutors(needsMultipleDifferentExecutorsForTasks);
				this.tasks.add(task);
			}
		}
		return this;
	}

	public void shuffleTasks() {
		synchronized (tasks) { // Don't want to execute the same tasks first always...
			Collections.shuffle(tasks);
		}
	}

	public Task nextExecutableTask() {
		Task task = tasks.get(taskPointer);
		taskPointer = (taskPointer + 1) % tasks.size();
		if (task.canBeExecuted()) {
			numberOfExecutions++;
			return task.incrementExecutionNumber().incrementActiveCount();
		} else {
			if (taskPointer == 0) {
				return null; // Nothing to execute anymore...
			} else {
				return nextExecutableTask(); // Try next one...
			}
		}
	}

	public int numberOfExecutions() {
		return this.numberOfExecutions;
	}

	public Task getTask(Task task) {
		if (tasks.contains(task)) {
			return tasks.get(tasks.indexOf(task));
		} else {
			return null;
		}
	}

	public int tasksSize() {
		return tasks.size();
	}

	public Object combiner() {
		return this.combiner;
	}

	public Procedure combiner(Object combiner) {
		this.combiner = combiner;
		return this;
	}

	public Procedure dataInputDomain(JobProcedureDomain dataInputDomain) {
		this.dataInputDomain = dataInputDomain;
		if (this.jobId == null) {
			this.jobId = dataInputDomain.jobId();
		}
		return this;
	}

	public JobProcedureDomain dataInputDomain() {
		return this.dataInputDomain;
	}

	@Override
	public JobProcedureDomain resultOutputDomain() {
		return (JobProcedureDomain) super.resultOutputDomain();
	}

	/**
	 * Set via dataInputDomain
	 * 
	 * @return
	 */
	public String jobId() {
		return jobId;
	}

	public Object executable() {
		return executable;
	}

	@Override
	public Procedure needsMultipleDifferentExecutors(boolean needsMultipleDifferentDomains) {
		return (Procedure) super.needsMultipleDifferentExecutors(needsMultipleDifferentDomains);
	}

	public Procedure needsMultipleDifferentExecutorsForTasks(boolean needsMultipleDifferentExecutorsForTasks) {
		this.needsMultipleDifferentExecutorsForTasks = needsMultipleDifferentExecutorsForTasks;
		synchronized (tasks) { // If it's set on the go, should update all tasks (hopefully never happens...)
			for (Task task : tasks) {
				task.needsMultipleDifferentExecutors(needsMultipleDifferentExecutorsForTasks);
			}
		}
		return this;
	}

	// END Setter/Getter
	@Override
	public Procedure clone() {
		Procedure procedure = null;
		try {
			procedure = (Procedure) super.clone();
			return procedure;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String toString() {
		return "Procedure [executable=" + executable + ", nrOfSameResultHashForTasks=" + nrOfSameResultHashForTasks + ", needsMultipleDifferentExecutorsForTasks="
				+ needsMultipleDifferentExecutorsForTasks + ", nrOfSameResultHash=" + nrOfSameResultHash + ", needsMultipleDifferentExecutors=" + needsMultipleDifferentExecutors + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((executable == null) ? 0 : executable.hashCode());
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + procedureIndex;
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
		Procedure other = (Procedure) obj;
		if (executable == null) {
			if (other.executable != null)
				return false;
		} else if (!executable.equals(other.executable))
			return false;
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (procedureIndex != other.procedureIndex)
			return false;
		return true;
	}

	public double taskSummarisationFactor() {
		return taskSummarisationFactor;
	}

	/**
	 * 
	 * @param taskSummarisationFactor
	 *            value between 0.0 and 1.0. 0.0 means that every finished task will be sent as message, 1.0 that only once a message is sent for all tasks. 0.0 increases reliability as less data may
	 *            be lost if a broadcast is not received. 1.0 will increase speed as it is only sent once, but all data may be lost if the broadcast is not received
	 * @return
	 */
	public Procedure taskSummarisationFactor(double taskSummarisationFactor) {
		if (taskSummarisationFactor < 0.0) {
			taskSummarisationFactor = 0.0;
		} else if (taskSummarisationFactor > 1.0) {
			taskSummarisationFactor = 1.0;
		}
		this.taskSummarisationFactor = taskSummarisationFactor;
		return this;
	}
}
