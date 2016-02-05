package mapreduce.engine.messageconsumers.updates;

import java.util.List;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;

public class TaskUpdate extends AbstractUpdate {
	// private static Logger logger = LoggerFactory.getLogger(TaskUpdate.class);

	private JobCalculationMessageConsumer msgConsumer;
	private List<ExecutorTaskDomain> outputDomains;

	public TaskUpdate(JobCalculationMessageConsumer msgConsumer, List<ExecutorTaskDomain> outputDomains) {
		this.msgConsumer = msgConsumer;
	}

	private TaskUpdate() {

	}

	@Override
	protected void internalUpdate(Procedure procedure) throws ClassCastException, NullPointerException {
		// ExecutorTaskDomain outputETD = (ExecutorTaskDomain) outputDomain;
		// TODO: parallelize?

		for (ExecutorTaskDomain outputETD : outputDomains) {
			Task receivedTask = Task.create(outputETD.taskId(), msgConsumer.executor().id());
			Task task = procedure.getTask(receivedTask);
			if (task == null) {
				task = receivedTask;
				procedure.addTask(task);
			}
			if (!task.isFinished()) {// Is finished before adding new output procedure domain? then ignore update
				task.addOutputDomain(outputETD);
				// Is finished anyways or after adding new output procedure domain? then abort any executions of this task and
				if (task.isFinished()) {
					// transfer the task's output <K,{V}> to the procedure domain
					msgConsumer.cancelTaskExecution(procedure.dataInputDomain().toString(), task); // If so, no execution needed anymore
					// Transfer data to procedure domain! This may cause the procedure to become finished
					msgConsumer.executor().switchDataFromTaskToProcedureDomain(procedure, task);
				}
			}
		}

	}

	public static IUpdate create(JobCalculationMessageConsumer msgConsumer, List<ExecutorTaskDomain> outputDomains) {
		return new TaskUpdate(msgConsumer, outputDomains);
	}
}
