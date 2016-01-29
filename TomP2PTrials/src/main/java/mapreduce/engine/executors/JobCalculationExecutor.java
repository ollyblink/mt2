package mapreduce.engine.executors;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class JobCalculationExecutor extends AbstractExecutor {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationExecutor.class);

	// private Map<String, ListMultimap<Task, BaseFuture>> futures;

	private JobCalculationExecutor() {
		super(IDCreator.INSTANCE.createTimeRandomID(JobCalculationExecutor.class.getSimpleName()));
	}

	public static JobCalculationExecutor create() {
		return new JobCalculationExecutor();
	}

	public void executeTask(Task task, Procedure procedure) {
		logger.info("executeTask: Task to execute: " + task);
		dhtConnectionProvider.getAll(task.key(), procedure.dataInputDomain().toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					List<Object> values = syncedArrayList();
					Set<Number640> valueSet = future.dataMap().keySet();
					for (Number640 valueHash : valueSet) {
						Object taskValue = ((Value) future.dataMap().get(valueHash).object()).value();
						values.add(taskValue);
					}

					JobProcedureDomain outputJPD = JobProcedureDomain.create(procedure.jobId(), procedure.dataInputDomain().jobSubmissionCount(), id, procedure.executable().getClass().getSimpleName(),
							procedure.procedureIndex());

					ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.newStatusIndex(), outputJPD);

					IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
					if (procedure.combiner() != null) {
						IContext combinerContext = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
						context.combiner((IExecutable) procedure.combiner(), combinerContext);
					}
					((IExecutable) procedure.executable()).process(task.key(), values, context);
					if (procedure.combiner() != null) {
						context.combine();
					}
					IContext contextToUse = (procedure.combiner() == null ? context : context.combinerContext());
					if (contextToUse.futurePutData().size() > 0) {
						Futures.whenAllSuccess(contextToUse.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
							@Override
							public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
								if (future.isSuccess()) {
									broadcastTaskCompletion(task, procedure, outputJPD, outputETD, contextToUse);
								} else {
									logger.warn("executeTask: No success on task execution. Reason: " + future.failedReason());
								}
							}

						});
					} else {// FuturePut data is 0 --> may happen if the Executable does not produce any results
						broadcastTaskCompletion(task, procedure, outputJPD, outputETD, contextToUse);
					}
				} else {
					logger.info("Could not retrieve data for task " + task.key() + " in job procedure domain: " + procedure.dataInputDomain().toString() + ". Failed reason: " + future.failedReason());
				}
			}

		});
	}

	private void broadcastTaskCompletion(Task task, Procedure procedure, JobProcedureDomain outputJPD, ExecutorTaskDomain outputETD, IContext contextToUse) {
		outputETD.resultHash(contextToUse.resultHash());
		// Adds it to itself, does not receive broadcasts... Makes sure this result is ignored in case another was received already dhtConnectionProvider.broadcastCompletion(msg);
		CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD, procedure.dataInputDomain().nrOfFinishedTasks(procedure.nrOfFinishedAndTransferredTasks()));
		// Adds it to itself, does not receive broadcasts... Makes sure this result is ignored in case another was received already
		dhtConnectionProvider.broadcastCompletion(msg);//
		dhtConnectionProvider.broadcastHandler().processMessage(msg, dhtConnectionProvider.broadcastHandler().getJob(outputJPD.jobId()));
		logger.info("executeTask: Successfully broadcasted TaskCompletedBCMessage for task " + task);
	}

	public void switchDataFromTaskToProcedureDomain(Procedure procedure, Task task) {
		if (task.isFinished() && !task.isInProcedureDomain()) {
			logger.info("switchDataFromTaskToProcedureDomain: Transferring task " + task + " to procedure domain ");
			List<FutureGet> futureGetKeys = syncedArrayList();
			List<FutureGet> futureGetValues = syncedArrayList();
			List<FuturePut> futurePuts = syncedArrayList();

			ExecutorTaskDomain fromETD = task.resultOutputDomain();

			JobProcedureDomain toJPD = JobProcedureDomain.create(procedure.jobId(), procedure.dataInputDomain().jobSubmissionCount(), id, procedure.executable().getClass().getSimpleName(),
					procedure.procedureIndex());
			// transferDataFromETDtoJPD(taskToTransfer, from, to, futureGetKeys, futureGetValues, futurePuts);
			
			futureGetKeys.add(dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, fromETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						Set<Number640> keySet = future.dataMap().keySet();
						for (Number640 n : keySet) {
							String taskOutputKey = (String) future.dataMap().get(n).object();
							logger.info("transferDataFromETDtoJPD:: taskOutputKey: " + taskOutputKey);
							futureGetValues.add(dhtConnectionProvider.getAll(taskOutputKey, fromETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

								@Override
								public void operationComplete(FutureGet future) throws Exception {
									if (future.isSuccess()) {
//										Collection<Data> values = future.dataMap().values();
//										List<Object> realValues = syncedArrayList();
//										for (Data d : values) {
//											realValues.add(((Value) d.object()).value());
//										}

										futurePuts.add(dhtConnectionProvider.addAll(taskOutputKey, future.dataMap().values(), toJPD.toString()).addListener(new BaseFutureAdapter<FuturePut>() {

											@Override
											public void operationComplete(FuturePut future) throws Exception {

												if (future.isSuccess()) {
													logger.info("transferDataFromETDtoJPD::Successfully added task output values of task output key \"" + taskOutputKey
															+ "\" for task " + task.key() + " to output procedure domain " + toJPD.toString());

												} else {
													logger.info("transferDataFromETDtoJPD::Failed to add values for task output key " + taskOutputKey + " to output procedure domain "
															+ toJPD.toString() + ", failed reason: " + future.failedReason());
												}
											}

										}));
										futurePuts.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskOutputKey, toJPD.toString(), false)
												.addListener(new BaseFutureAdapter<FuturePut>() {

											@Override
											public void operationComplete(FuturePut future) throws Exception {
												if (future.isSuccess()) {
													logger.info("transferDataFromETDtoJPD::Successfully added task output key \"" + taskOutputKey + "\" for task " + task.key()
															+ " to output procedure domain " + toJPD.toString());
												} else {
													logger.info("transferDataFromETDtoJPD::Failed to add task output key and values for task output key \"" + taskOutputKey + "\" for task "
															+ task.key() + " to output procedure domain " + toJPD.toString() + ", failed reason: " + future.failedReason());
												}
											}

										}));

									} else {
										logger.info("transferDataFromETDtoJPD::Failed to get task output key and values for task output key (" + taskOutputKey + " from task executor domain "
												+ fromETD.toString() + ", failed reason: " + future.failedReason());

									}
								}
							}));

						}
					} else {
						logger.warn("transferDataFromETDtoJPD::Failed to get task keys for task " + task.key() + " from task executor domain " + fromETD.toString() + ", failed reason: "
								+ future.failedReason());
					}
				}
			}));
			logger.info("switchDataFromTaskToProcedureDomain:: futureGetKeys.size():  " + futureGetKeys.size());

			Futures.whenAllSuccess(futureGetKeys).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

				@Override
				public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
					if (future.isSuccess()) {
						logger.info("switchDataFromTaskToProcedureDomain::futureGetValues.size(): " + futureGetValues.size());
						if (futureGetValues.size() > 0) {
							Futures.whenAllSuccess(futureGetValues).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

								@Override
								public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
									if (future.isSuccess()) {
										logger.info("switchDataFromTaskToProcedureDomain::futurePuts.size(): " + futurePuts.size());
										Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

											@Override
											public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
												if (future.isSuccess()) {
													broadcastProcedureCompleted(procedure, task, toJPD);
												} else {
													logger.warn("switchDataFromTaskToProcedureDomain:: Failed to transfered task output keys and values for task " + task
															+ " from task executor domain to job procedure domain: " + toJPD.toString() + ". failed reason: " + future.failedReason());
												}
											}

										});
									} else {
										logger.warn("switchDataFromTaskToProcedureDomain::Failed to get task values for task " + task + " from task executor domain. failed reason: "
												+ future.failedReason());
									}
								}

							});
						} else { // This may happen if no output was produced --> no data to transfer from ETD to JPD
							logger.info("switchDataFromTaskToProcedureDomain:: else part... there was nothing to transfer because the task [" + task + "] did not produce any output");
							broadcastProcedureCompleted(procedure, task, toJPD);
						}
					} else {
						logger.warn("switchDataFromTaskToProcedureDomain::Failed to get task keys for task " + task + " from task executor domain. failed reason: " + future.failedReason());
					}
				}

			});
		}
	}

	private void broadcastProcedureCompleted(Procedure procedure, Task taskToTransfer, JobProcedureDomain to) {
		taskToTransfer.isInProcedureDomain(true);
		logger.info("broadcastProcedureCompleted:: Successfully transfered task output keys and values for task " + taskToTransfer + " from task executor domain to job procedure domain: "
				+ to.toString() + ". ");

		CompletedBCMessage msg = tryCompletingProcedure(procedure);
		if (msg != null) {
			dhtConnectionProvider.broadcastCompletion(msg);
			dhtConnectionProvider.broadcastHandler().processMessage(msg, dhtConnectionProvider.broadcastHandler().getJob(procedure.jobId()));
			logger.info("broadcastProcedureCompleted:: Broadcasted Completed Procedure MSG: " + msg);
		}
	}

	public CompletedBCMessage tryCompletingProcedure(Procedure procedure) {
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		int expectedSize = dataInputDomain.expectedNrOfFiles();
		// List<Task> tasks = procedure.tasks();
		int currentSize = procedure.tasksSize();
		logger.info("tryCompletingProcedure: data input domain procedure: " + dataInputDomain.procedureSimpleName());
		logger.info("tryCompletingProcedure: expectedSize == currentSize? " + expectedSize + "==" + currentSize);
		if (expectedSize == currentSize) {
			if (procedure.isCompleted()) {
				JobProcedureDomain outputProcedure = JobProcedureDomain
						.create(procedure.jobId(), dataInputDomain.jobSubmissionCount(), id, procedure.executable().getClass().getSimpleName(), procedure.procedureIndex())
						.resultHash(procedure.resultHash()).expectedNrOfFiles(currentSize);
				logger.info("tryCompleteProcedure:: new output procedure is: " + outputProcedure);
				CompletedBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(outputProcedure, dataInputDomain);
				return msg;
			}
		}
		return null;
	}

	// private void transferDataFromETDtoJPD(Task task, ExecutorTaskDomain fromETD, JobProcedureDomain toJPD, List<FutureGet> futureGetKeys, List<FutureGet> futureGetValues, List<FuturePut>
	// futurePuts) {
	//
	//
	//
	// }

	@Override
	public JobCalculationExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public JobCalculationExecutor performanceInformation(PerformanceInfo performanceInformation) {
		this.performanceInformation = performanceInformation;
		return this;
	}

}
