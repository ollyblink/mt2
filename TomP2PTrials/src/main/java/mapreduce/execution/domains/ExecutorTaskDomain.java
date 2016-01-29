package mapreduce.execution.domains;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.utils.DomainProvider;
import net.tomp2p.peers.Number160;

public class ExecutorTaskDomain implements IDomain {
	private static Logger logger = LoggerFactory.getLogger(ExecutorTaskDomain.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -4306577142597619221L;
	private String taskId;
	private String taskExecutor;
	private int taskStatusIndex;
	private Number160 resultHash;
	private JobProcedureDomain jobProcedureDomain;

	public static ExecutorTaskDomain create(String taskId, String taskExecutor, int taskStatusIndex,
			JobProcedureDomain jobProcedureDomain) {
		return new ExecutorTaskDomain(taskId, taskExecutor, taskStatusIndex, jobProcedureDomain);
	}

	private ExecutorTaskDomain() {

	}

	private ExecutorTaskDomain(String taskId, String taskExecutor, int taskStatusIndex,
			JobProcedureDomain jobProcedureDomain) {
		this.taskId = taskId;
		this.taskExecutor = taskExecutor;
		this.taskStatusIndex = taskStatusIndex;
		this.jobProcedureDomain = jobProcedureDomain;
		this.resultHash = null;
	}

	@Override
	public Number160 resultHash() {
		return resultHash;
	}

	@Override
	public String executor() {
		return taskExecutor;
	}

	public String taskId() {
		return taskId;
	}

	public int taskStatusIndex() {
		return taskStatusIndex;
	}

	public JobProcedureDomain jobProcedureDomain() {
		return jobProcedureDomain;
	}

	@Override
	public String toString() {
		return DomainProvider.INSTANCE.concatenation(jobProcedureDomain, this);
	}

	@Override
	public ExecutorTaskDomain resultHash(Number160 resultHash) {
		logger.info("resultHash: "+ resultHash);
		this.resultHash = resultHash;
		return this;
	}

	@Override
	public ExecutorTaskDomain clone() {
		ExecutorTaskDomain etd = null;
		try {
			etd = (ExecutorTaskDomain) super.clone();
			etd.jobProcedureDomain = jobProcedureDomain.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return etd;

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobProcedureDomain == null) ? 0 : jobProcedureDomain.hashCode());
		result = prime * result + ((taskExecutor == null) ? 0 : taskExecutor.hashCode());
		result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
		result = prime * result + taskStatusIndex;
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
		ExecutorTaskDomain other = (ExecutorTaskDomain) obj;
		if (jobProcedureDomain == null) {
			if (other.jobProcedureDomain != null)
				return false;
		} else if (!jobProcedureDomain.equals(other.jobProcedureDomain))
			return false;
		if (taskExecutor == null) {
			if (other.taskExecutor != null)
				return false;
		} else if (!taskExecutor.equals(other.taskExecutor))
			return false;
		if (taskId == null) {
			if (other.taskId != null)
				return false;
		} else if (!taskId.equals(other.taskId))
			return false;
		if (taskStatusIndex != other.taskStatusIndex)
			return false;
		return true;
	}

}