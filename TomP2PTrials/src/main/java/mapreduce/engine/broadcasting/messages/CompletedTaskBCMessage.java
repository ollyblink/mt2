package mapreduce.engine.broadcasting.messages;

import java.io.Serializable;
import java.util.List;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public class CompletedTaskBCMessage extends AbstractBCMessage {

	private static final long serialVersionUID = -8190226477312977951L;

	private final class TaskResultTriple implements Serializable {

		private static final long serialVersionUID = -8952996521399624074L;

		private TaskResultTriple() {
		}

		private TaskResultTriple(String taskId, String taskExecutor, int taskExecutionNumber, Number160 resultHash) {
			this.taskId = taskId;
			this.taskExecutor = taskExecutor;
			this.taskExecutionNumber = taskExecutionNumber;
			this.resultHash = resultHash;
		}

		private String taskId;
		private String taskExecutor;
		private int taskExecutionNumber;
		private Number160 resultHash;

		@Override
		public String toString() {
			return " [taskId=" + taskId + "] ";
		}

	}

	private final List<TaskResultTriple> taskResultTriples = SyncedCollectionProvider.syncedArrayList();

	private CompletedTaskBCMessage(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		super(outputDomain, inputDomain);
	}

	public static CompletedTaskBCMessage create(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		return new CompletedTaskBCMessage(outputDomain, inputDomain);
	}

	@Override
	public BCMessageStatus status() {
		return BCMessageStatus.COMPLETED_TASK;
	}

	public CompletedTaskBCMessage addOutputDomainTriple(ExecutorTaskDomain outputETD) {
		if (outputDomain().equals(outputETD.jobProcedureDomain())) {
			this.taskResultTriples.add(new TaskResultTriple(outputETD.taskId(), outputETD.executor(), outputETD.taskExecutionNumber(), outputETD.resultHash()));
		}
		return this;
	}

	public List<ExecutorTaskDomain> allExecutorTaskDomains() {
		List<ExecutorTaskDomain> etds = SyncedCollectionProvider.syncedArrayList();
		for (TaskResultTriple t : taskResultTriples) {
			etds.add(ExecutorTaskDomain.create(t.taskId, t.taskExecutor, t.taskExecutionNumber, outputDomain()).resultHash(t.resultHash));
		}
		return etds;
	}

	@Override
	public String toString() {
		return "CompletedTaskBCMessage [taskResultTriples=" + taskResultTriples + "]";
	}

}
