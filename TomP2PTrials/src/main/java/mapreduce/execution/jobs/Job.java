package mapreduce.execution.jobs;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.utils.FileSize;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;

public class Job implements Serializable, Cloneable {
	private static Logger logger = LoggerFactory.getLogger(Job.class);
	private static final long serialVersionUID = 1152022246679324578L;
	public static final PriorityLevel DEFAULT_PRIORITY_LEVEL = PriorityLevel.MODERATE;
	public static final FileSize DEFAULT_MAX_FILE_SIZE = FileSize.MEGA_BYTE;
	public static final boolean DEFAULT_USE_LOCAL_STORAGE_FIRST = false;
	public static final long DEFAULT_TIME_TO_LIVE = 10000;
	public static final String DEFAULT_RESULT_OUTPUT_FOLDER = System.getProperty("user.dir") + "/tmp/";
	public static final FileSize DEFAULT_RESULT_OUTPUT_FILESIZE = FileSize.MEGA_BYTE;
	public static final String DEFAULT_FILE_ENCODING = "UTF-8";

	/** The folder used to store the result data of the last procedure */
	private String resultOutputFolder = DEFAULT_RESULT_OUTPUT_FOLDER;
	/** How large the retrieved files can grow */
	private FileSize outputFileSize = DEFAULT_RESULT_OUTPUT_FILESIZE;
	/** specifies a unique id for this job */
	private final String id;
	/** identifier for the submitting entity (@see{MRJobSubmissionManager}) */
	private final String jobSubmitterID;
	/** Used for order of jobs @see{<code>PriorityLevel</code> */
	private final PriorityLevel priorityLevel;
	/** Used for order of jobs. System.currentTimeInMillis(): long */
	private final Long creationTime;
	/**
	 * Contains all procedures for this job. Processing is done from 0 to procedures.size()-1, meaning that the first procedure added using Job.nextProcedure(procedure) is also the first one to be
	 * processed.
	 */
	private List<Procedure> procedures;
	/**
	 * Internal counter that specifies the currently processed procedure
	 */
	private int currentProcedureIndex;
	/** maximal file size to be put on the DHT at once */
	private FileSize maxFileSize;
	/**
	 * Where the data files for the first procedure is stored
	 */
	private String fileInputFolderPath;
	/** input file encoding */
	private String fileEncoding;
	/**
	 * if true, the peer tries to pull tasks from own storage before accessing the dht. If false, locality of data is ignored and instead the dht is directly accessed in all cases (possibly slower)
	 */
	private boolean useLocalStorageFirst;
	/** private counter to specify the number of times this job has already been submitted, to avoid having to retrieve the same job again */
	private int submissionCounter = 0;
	/** used by the submitting entity to mark this job as truly finished */
	private boolean isRetrieved;
	/** How many times should the job be resubmitted in case time ran out until a new bc message was received? */
	private int maxNrOfSubmissionTrials = 1;

	private class TimeoutSpecification implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -4276507265165258443L;

		/** How long a node should be kept alive after the last received message for this job */
		private long timeToLive;
		/** guesses the timeout according to the differences between received timeouts (plus a fraction of the time added) */
		private boolean guessTimeout;
		/** fraction of how much time should be added to the difference of the guessed timeout to accommodate the fact that it may take longer each time a message was received */
		private double fraction;

		public TimeoutSpecification(long timeToLive, boolean guessTimeout, double fraction) {
			this.timeToLive = timeToLive;
			this.guessTimeout = guessTimeout;
			this.fraction = fraction;
		}

	}

	private TimeoutSpecification submitterTimeoutSpecification;
	private TimeoutSpecification calculatorTimeoutSpecification;

	private Job(String jobSubmitterID, PriorityLevel priorityLevel) {
		this.jobSubmitterID = jobSubmitterID;
		this.id = IDCreator.INSTANCE.createTimeRandomID(this.getClass().getSimpleName());
		this.priorityLevel = (priorityLevel == null ? DEFAULT_PRIORITY_LEVEL : priorityLevel);
		this.creationTime = System.currentTimeMillis();
		this.currentProcedureIndex = 0;
		this.procedures = SyncedCollectionProvider.syncedArrayList();
		this.addSucceedingProcedure(StartProcedure.create(), null, 1, 1, false, false, 0.0);// Add initial
		this.addSucceedingProcedure(EndProcedure.create(), null, 0, 0, false, false, 0.0); // add trailer
	}

	public static Job create(String jobSubmitterID) {
		return create(jobSubmitterID, DEFAULT_PRIORITY_LEVEL);
	}

	public static Job create(String jobSubmitterID, PriorityLevel priorityLevel) {
		return new Job(jobSubmitterID, priorityLevel).fileInputFolderPath(null, DEFAULT_FILE_ENCODING).maxFileSize(DEFAULT_MAX_FILE_SIZE).useLocalStorageFirst(DEFAULT_USE_LOCAL_STORAGE_FIRST)
				.submitterTimeoutSpecification(DEFAULT_TIME_TO_LIVE, false, 0.0).calculatorTimeoutSpecification(DEFAULT_TIME_TO_LIVE / 2, false, 0.0);
		// calculator should live half the time the submitter lives in case the time out of the calculator
		// SHOULD
		// be reached (e.g. when the expected nr of files is not the same as the actual number of files, which
		// may happen in the beginning when the nr of files is only guessed, but the actual splitting results
		// in more files --> see JobSubmissionExecutor.submit and .estimateNrOfFiles

	}

	public Job fileInputFolderPath(String fileInputFolderPath, String fileEncoding) {
		this.fileInputFolderPath = fileInputFolderPath;
		this.fileEncoding = fileEncoding;
		return this;
	}

	public Job maxFileSize(FileSize maxFileSize) {
		this.maxFileSize = maxFileSize;
		return this;
	}

	public Job useLocalStorageFirst(boolean useLocalStorageFirst) {
		this.useLocalStorageFirst = useLocalStorageFirst;
		return this;
	}

	// Getters
	public boolean isFinished() {
		for (Procedure procedure : procedures) {
			if (!procedure.isFinished()) {
				return false;
			}
		}
		return true;
	}

	public PriorityLevel priorityLevel() {
		return priorityLevel;
	}

	public Long creationTime() {
		return creationTime;
	}

	public String fileInputFolderPath() {
		return fileInputFolderPath;
	}

	public FileSize maxFileSize() {
		return this.maxFileSize;
	}

	public boolean useLocalStorageFirst() {
		return this.useLocalStorageFirst;
	}

	public String id() {
		// S == Submitter
		return this.id + "_S(" + jobSubmitterID + ")";
	}

	public String jobSubmitterID() {
		return this.jobSubmitterID;
	}

	// FUNCTIONALITY
	/**
	 * Returns the procedure at the specified index. As procedures are added using Job.nextProcedure(procedure), the index in the list they are stored in also specifies the order in which procedures
	 * are processed. As such, the index starts from 0 (first procedure) and ends with list.size()-1 (last procedure to be processed). Use this method together with Job.currentProcedureIndex() to
	 * access the currently processed procedure. E.g. Job.procedure(Job.currentProcedureIndex())
	 * 
	 * @param index
	 * @return the procedure at the specified index
	 */
	public Procedure procedure(int index) {
		if (index < 0) {
			return procedures.get(0);
		} else if (index >= procedures.size() - 1) {
			return procedures.get(procedures.size() - 1);
		} else {
			return procedures.get(index);
		}
	}

	/**
	 * Convenience method. Same as Job.procedure(Job.currentProcedureIndex()+1). Retrieves the procedure information for the next procedure to be executed after the current procedure
	 * 
	 * @return
	 */
	public Procedure currentProcedure() {
		return this.procedure(this.currentProcedureIndex);
	}

	/**
	 * Adds a procedure to the job. The idea is that chaining of nextProcedure() calls (e.g. Job.nextProcedure(...).nextProcedure(...) also indicates how procedures are processed (from 0 to N). This
	 * means that the first procedure added is also the first procedure that is going to be processed, until the last added procedure that was added.
	 * 
	 * @param procedure
	 *            the actual procedure to execute next
	 * @param combiner
	 *            combines data for this procedure before sending it to the dht. If combiner is null, no combination is done before sending the data to the dht. Often, this is the same as the
	 *            subsequent procedure following this procedure, only applied locally
	 * @param nrOfSameResultHashForProcedure
	 *            specifies how many times this procedure should achieve the same result hash before one is confident enough to consider the procedure to be finished
	 * @param needsMultipledDifferentResulthashsForTasks
	 * @param needsMultipledDifferentResulthashs
	 * @param numberOfSameResultHashForTasks
	 *            specifies how many times the tasks of this procedure should achieve the same result hash before one is confident enough to consider a task to be finished
	 * @param needsMultipleDifferentExecutors
	 *            specifies if the procedure needs to be executed by different executors to become finished
	 * @param needMultipleDifferentDomainsForTask
	 *            specifies if tasks need to be executed by different executors to become finished
	 * @param taskSummarisationFactor
	 *            Factor used especially in task intensive jobs, where each task is rather small and quick to calculate, to summarise the number of results sent. Reduces number of sent messages over
	 *            network but increases the risk of losing results in case a broadcast is not received. Used as a fraction of the total number of task (default is 0.0, meaning that every finished task
	 *            is sent as a message. 1.0 means that only one message is sent when all tasks finished)
	 * @return
	 */
	public Job addSucceedingProcedure(Object procedure, Object combiner, int nrOfSameResultHashForProcedure, int nrOfSameResultHashForTasks, boolean needsMultipleDifferentExecutors,
			boolean needsMultipleDifferentExecutorsForTasks, double taskSummarisationFactor) {
		if (procedure == null) {
			return this;
		}
		nrOfSameResultHashForProcedure = (nrOfSameResultHashForProcedure < 0 ? 0 : nrOfSameResultHashForProcedure);
		nrOfSameResultHashForTasks = (nrOfSameResultHashForTasks < 0 ? 0 : nrOfSameResultHashForTasks);

		Procedure procedureInformation = Procedure.create(procedure, -1).nrOfSameResultHash(nrOfSameResultHashForProcedure).needsMultipleDifferentExecutors(needsMultipleDifferentExecutors)
				.nrOfSameResultHashForTasks(nrOfSameResultHashForTasks).needsMultipleDifferentExecutorsForTasks(needsMultipleDifferentExecutorsForTasks).combiner(combiner)
				.taskSummarisationFactor(taskSummarisationFactor);

		logger.info("addSucceedingProcedure::Procedure is [" + procedureInformation.toString() + "]");
		if (this.procedures.size() < 2) {
			this.procedures.add(procedureInformation.procedureIndex(procedures.size()));
		} else {
			this.procedures.add(this.procedures.size() - 1, procedureInformation.procedureIndex(procedures.size() - 1));
			// Adapting last procedure index (EndProcedure)
			this.procedures.get(this.procedures.size() - 1).procedureIndex(this.procedures.size() - 1);
		}
		return this;
	}

	public Job addSucceedingProcedure(Object executable) {
		return addSucceedingProcedure(executable, null, 1, 1, false, false, 0.0);
	}

	public Job addSucceedingProcedure(Object executable, Object combiner) {
		return addSucceedingProcedure(executable, combiner, 1, 1, false, false, 0.0);
	}

	public Job addSucceedingProcedure(Object executable, Object combiner, double taskSummarisationFactor) {
		return addSucceedingProcedure(executable, combiner, 1, 1, false, false, taskSummarisationFactor);
	}

	public Job addSucceedingProcedure(Object executable, Object combiner, int nrOfSameResultHashForProcedure, int nrOfSameResultHashForTasks, boolean needsMultipleDifferentExecutors,
			boolean needsMultipleDifferentExecutorsForTasks) {
		return addSucceedingProcedure(executable, combiner, nrOfSameResultHashForProcedure, nrOfSameResultHashForTasks, needsMultipleDifferentExecutors, needsMultipleDifferentExecutorsForTasks, 0.0);
	}

	// private Procedure createProcedure(Object procedure, Object combiner, int nrOfSameResultHashForProcedure, int nrOfSameResultHashForTasks, boolean needsMultipleDifferentExecutors,
	// boolean needsMultipleDifferentExecutorsForTasks, double taskSummarisationFactor) {
	//
	// return procedureInformation;
	// }
	//
	// /**
	// * In case you prefer writing the function in javascript instead...
	// *
	// * @param javaScriptProcedure
	// * @param javaScriptCombiner
	// * @param nrOfSameResultHashForProcedure
	// * @param nrOfSameResultHashForTasks
	// * @param needMultipleDifferentDomains
	// * @param needMultipleDifferentDomainsForTasks
	// * @return
	// */
	// public Job addSucceedingProcedure(String javaScriptProcedure, String javaScriptCombiner, int nrOfSameResultHashForProcedure, int nrOfSameResultHashForTasks, boolean
	// needMultipleDifferentDomains,
	// boolean needMultipleDifferentDomainsForTasks, double taskSummarisationFactor) {
	// if (javaScriptProcedure != null && javaScriptProcedure.length() == 0) {
	// return this;
	// }
	//
	// return addSucceedingProcedure((Object) javaScriptProcedure, (Object) javaScriptCombiner, nrOfSameResultHashForProcedure, nrOfSameResultHashForTasks, needMultipleDifferentDomains,
	// needMultipleDifferentDomainsForTasks, taskSummarisationFactor);
	// }

	public Job incrementProcedureIndex() {
		if (this.currentProcedureIndex <= procedures.size()) {
			logger.info("incrementProcedureIndex, this.currentProcedureIndex <= procedures.size(), currentProcedure().clear() called");
			currentProcedure().clear();// Considered finished, else it would not have been called... Thus, don't need the tasks anymore...
			++this.currentProcedureIndex;
		}
		return this;
	}

	@Override
	public String toString() {
		return id();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id() == null) ? 0 : id().hashCode());
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
		Job other = (Job) obj;
		if (id() == null) {
			if (other.id() != null)
				return false;
		} else if (!id().equals(other.id()))
			return false;
		return true;
	}

	//
	// public boolean isActive() {
	// return isActive;
	// }
	//
	// public Job isActive(boolean isActive) {
	// this.isActive = isActive;
	// return this;
	// }

	@Override
	public Job clone() {

		try {
			Job job = (Job) super.clone();

			// job.creationTime = creationTime;
			// job.currentProcedureIndex = currentProcedureIndex;
			// job.fileInputFolderPath = fileInputFolderPath;
			// job.id = id;
			// job.jobSubmitterID = jobSubmitterID;
			// job.maxFileSize = maxFileSize;
			// job.nrOfSameResultHash = nrOfSameResultHash;
			// job.priorityLevel = priorityLevel;
			// job.useLocalStorageFirst = useLocalStorageFirst;
			job.procedures = SyncedCollectionProvider.syncedArrayList();
			for (Procedure p : procedures) {
				job.procedures.add(p.clone());
			}
			return job;
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<Procedure> procedures() {
		return procedures;
	}

	public int incrementSubmissionCounter() {
		return ++this.submissionCounter;
	}

	public int submissionCount() {
		return this.submissionCounter;
	}

	public boolean isRetrieved() {
		return isRetrieved;
	}

	public Job isRetrieved(boolean isRetrieved) {
		this.isRetrieved = isRetrieved;
		return this;
	}

	public int maxNrOfSubmissionTrials() {
		return maxNrOfSubmissionTrials;
	}

	public Job maxNrOfSubmissionTrials(int maxNrOfSubmissionTrials) {
		this.maxNrOfSubmissionTrials = maxNrOfSubmissionTrials;
		return this;
	}

	public String resultOutputFolder() {
		return resultOutputFolder;
	}

	public FileSize outputFileSize() {
		return this.outputFileSize;
	}

	public Job resultOutputFolder(String resultOutputFolder, FileSize outputFileSize) {
		this.resultOutputFolder = resultOutputFolder + "/tmp";
		this.outputFileSize = outputFileSize;
		return this;
	}

	public Job submitterTimeoutSpecification(long timeToLive, boolean guessTimeout, double fraction) {
		this.submitterTimeoutSpecification = new TimeoutSpecification(timeToLive, guessTimeout, fraction);
		return this;
	}

	public long submitterTimeToLive() {
		return this.submitterTimeoutSpecification.timeToLive;
	}

	public boolean submitterGuessTimeout() {
		return this.submitterTimeoutSpecification.guessTimeout;
	}

	public double submitterTimeoutFraction() {
		return this.submitterTimeoutSpecification.fraction;
	}

	public Job calculatorTimeoutSpecification(long timeToLive, boolean guessTimeout, double fraction) {
		this.calculatorTimeoutSpecification = new TimeoutSpecification(timeToLive, guessTimeout, fraction);
		return this;
	}

	public long calculatorTimeToLive() {
		return calculatorTimeoutSpecification.timeToLive;
	}

	public boolean calculatorGuessTimeout() {
		return calculatorTimeoutSpecification.guessTimeout;
	}

	public double calculatorTimeoutFraction() {
		return calculatorTimeoutSpecification.fraction;
	}

	public String fileEncoding() {
		return fileEncoding;
	}

}
