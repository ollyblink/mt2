package mapreduce.execution.domains;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.utils.DomainProvider;
import net.tomp2p.peers.Number160;

public class JobProcedureDomain implements IDomain {
	private static Logger logger = LoggerFactory.getLogger(JobProcedureDomain.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1860215470614190272L;
	private String jobId;
	private String procedureExecutor;
	private String procedureSimpleName;
	private int procedureIndex;
	private Number160 resultHash;
	/**
	 * Number of tasks for this procedure (may be different from tasks.size() because tasks are pulled after
	 * another and not all at the same time). When the preceding procedure finishes, it will add the number of
	 * task's (==tasksSize) such that the next procedure knows how many tasks there are to be processed.
	 */
	private volatile int expectedNrOfFiles;
	/**
	 * This data item is simply here for the MessageConsumer to decide which result to take if two executors
	 * execute a procedure on different input domains
	 */
	private int nrOfFinishedTasks;
	private int jobSubmissionCount;
	private PerformanceInfo executorPerformanceInformation;
	private boolean isJobFinished;

	private JobProcedureDomain() {

	}

	public static JobProcedureDomain create(String jobId, int jobSubmissionCount, String procedureExecutor,
			String procedureSimpleName, int procedureIndex) {
		return new JobProcedureDomain(jobId, jobSubmissionCount, procedureExecutor, procedureSimpleName,
				procedureIndex);
	}

	private JobProcedureDomain(String jobId, int jobSubmissionCount, String procedureExecutor,
			String procedureSimpleName, int procedureIndex) {
		this.jobId = jobId;
		this.jobSubmissionCount = jobSubmissionCount;
		this.procedureExecutor = procedureExecutor;
		this.procedureSimpleName = procedureSimpleName;
		this.procedureIndex = procedureIndex;
		this.resultHash = null;
	}

	@Override
	public Number160 resultHash() {
		return resultHash;
	}

	@Override
	public JobProcedureDomain resultHash(Number160 resultHash) {
		logger.info("resultHash: " + resultHash);
		this.resultHash = resultHash;
		return this;
	}

	@Override
	public String executor() {
		return procedureExecutor;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
		result = prime * result + ((procedureExecutor == null) ? 0 : procedureExecutor.hashCode());
		result = prime * result + procedureIndex;
		result = prime * result + ((procedureSimpleName == null) ? 0 : procedureSimpleName.hashCode());
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
		JobProcedureDomain other = (JobProcedureDomain) obj;
		if (jobId == null) {
			if (other.jobId != null)
				return false;
		} else if (!jobId.equals(other.jobId))
			return false;
		if (procedureExecutor == null) {
			if (other.procedureExecutor != null)
				return false;
		} else if (!procedureExecutor.equals(other.procedureExecutor))
			return false;
		if (procedureIndex != other.procedureIndex)
			return false;
		if (procedureSimpleName == null) {
			if (other.procedureSimpleName != null)
				return false;
		} else if (!procedureSimpleName.equals(other.procedureSimpleName))
			return false;
		return true;
	}

	public String jobId() {
		return this.jobId;
	}

	public String procedureSimpleName() {
		return this.procedureSimpleName;
	}

	public Integer procedureIndex() {
		return this.procedureIndex;
	}

	@Override
	public String toString() {
		return DomainProvider.INSTANCE.jobProcedureDomain(this);
	}

	@Override
	public JobProcedureDomain clone() {
		JobProcedureDomain jpd = null;
		try {
			jpd = (JobProcedureDomain) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return jpd;
	}

	public int expectedNrOfFiles() {
		return expectedNrOfFiles;
	}

	public JobProcedureDomain expectedNrOfFiles(int expectedNrOfFiles) {
		logger.info("expectedNrOfFiles:: before: " + this.expectedNrOfFiles + ", updated to: "
				+ expectedNrOfFiles);
		this.expectedNrOfFiles = expectedNrOfFiles;
		return this;
	}

	public int nrOfFinishedTasks() {
		return this.nrOfFinishedTasks;
	}

	public JobProcedureDomain nrOfFinishedTasks(int nrOfFinishedTasks) {
		this.nrOfFinishedTasks = nrOfFinishedTasks;
		return this;
	}

	public int jobSubmissionCount() {
		return this.jobSubmissionCount;
	}

	public PerformanceInfo executorPerformanceInformation() {
		return this.executorPerformanceInformation;
	}

	public JobProcedureDomain executorPerformanceInformation(PerformanceInfo executorPerformanceInformation) {
		this.executorPerformanceInformation = executorPerformanceInformation;
		return this;
	}

	public JobProcedureDomain isJobFinished(boolean isJobFinished) {
		this.isJobFinished = isJobFinished;
		return this;
	}

	public boolean isJobFinished() {
		return this.isJobFinished;
	}
}