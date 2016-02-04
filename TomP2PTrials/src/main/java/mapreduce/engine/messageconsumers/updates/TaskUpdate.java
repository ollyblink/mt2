package mapreduce.engine.messageconsumers.updates;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;

public class TaskUpdate extends AbstractUpdate {
//	private static Logger logger = LoggerFactory.getLogger(TaskUpdate.class);

	private JobCalculationMessageConsumer msgConsumer;

	public TaskUpdate(JobCalculationMessageConsumer msgConsumer) {
		this.msgConsumer = msgConsumer;
	}

	@Override
	protected void internalUpdate(IDomain outputDomain, Procedure procedure) throws ClassCastException, NullPointerException {
		ExecutorTaskDomain outputETD = (ExecutorTaskDomain) outputDomain;
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
