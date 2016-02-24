package mapreduce.engine.messageconsumers.updates;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;

public class TaskUpdate extends AbstractUpdate {
	private static Logger logger = LoggerFactory.getLogger(TaskUpdate.class);

	private JobCalculationMessageConsumer msgConsumer;
	private List<ExecutorTaskDomain> outputDomains;

	public TaskUpdate(JobCalculationMessageConsumer msgConsumer, List<ExecutorTaskDomain> outputDomains) {
		this.msgConsumer = msgConsumer;
		this.outputDomains = outputDomains;
	}

	@Override
	protected void internalUpdate(Procedure procedure) throws NullPointerException {
		// ExecutorTaskDomain outputETD = (ExecutorTaskDomain) outputDomain;
		// TODO: parallelize?
		synchronized (outputDomains) {
			for (ExecutorTaskDomain outputETD : outputDomains) {
				Task receivedTask = Task.create(outputETD.taskId(), JobCalculationExecutor.classId);
				Task task = procedure.getTask(receivedTask);
				if (task == null) {
					task = receivedTask;
					procedure.addTask(task);

				}
				logger.info("internalUpdate:: " + (outputDomains.get(0).executor().equals(JobCalculationExecutor.classId) ? " I received output domain is from myself"
						: " I received output domain is from other [" + outputDomains.get(0).executor() + "]"));

				// if (JobCalculationExecutor.classId.equals(outputETD.executor())) {
				// logger.info("internalUpdate:: I received an up-to-date message from myself for task: " + outputETD.taskId());
				// } else {
				// logger.info(
				// "internalUpdate:: I (" + JobCalculationExecutor.classId + ") received an up-to-date message from executor (" + outputETD.executor() + ") for task: " + outputETD.taskId());
				// }
				if (!task.isFinished()) {// Is finished before adding new output procedure domain? then ignore update
					task.addOutputDomain(outputETD);
					// Is finished anyways or after adding new output procedure domain? then abort any executions of this task and
					if (task.isFinished()) {
						// transfer the task's output <K,{V}> to the procedure domain
						msgConsumer.cancelTaskExecution(procedure.dataInputDomain().toString(), task);
						// TODO: Cancel incoming messages for task in jobCalcBCHandler
						// logger.info("internalUpdate: switchDataFromTaskToProcedureDomain");
						// Transfer data to procedure domain! This may cause the procedure to become finished
						// New
						msgConsumer.tryTransfer(procedure, task);
						// //Old
						// JobCalculationExecutor.create().dhtConnectionProvider(msgConsumer.dhtConnectionProvider()).switchDataFromTaskToProcedureDomain(procedure, task);
					} else {

					}
				}
			}
		}
		// logger.info("After taskupdate");

	}

	public static IUpdate create(JobCalculationMessageConsumer msgConsumer, List<ExecutorTaskDomain> outputDomains) {
		return new TaskUpdate(msgConsumer, outputDomains);
	}
}
