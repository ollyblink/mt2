package mapreduce.engine.executors;

import static mapreduce.utils.SyncedCollectionProvider.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessage;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Cancel;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class JobCalculationExecutor extends AbstractExecutor implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationExecutor.class);
	/**
	 * Well thats just a simple way to ensure all calculation executors have the same id on one node... easier than refactoring but should be done... In case sb wants more than one executor on a
	 * computer (although that only makes sense for testing purposes)
	 */
	public static String classId = IDCreator.INSTANCE.createTimeRandomID(JobCalculationExecutor.class.getSimpleName());
	private static ListMultimap<JobProcedureDomain, ExecutorTaskDomain> intermediate = SyncedCollectionProvider.syncedArrayListMultimap();
	private int numberOfExecutions = 1;
	private static Map<JobProcedureDomain, Integer> submitted = syncedHashMap();
	private ListMultimap<Task, BaseFuture> futuresForTask = syncedArrayListMultimap();
	private Task task;
	private Procedure procedure;
	private Job job;
	private volatile boolean canExecute = true;

	/** oly to decide which method to call in run... */
	private boolean isExecutor;
	private static Map<JobProcedureDomain, Boolean> locks = syncedHashMap();

	private JobCalculationExecutor() {
		super(classId);
	}

	public static JobCalculationExecutor create() {
		return new JobCalculationExecutor();
	}

	private JobCalculationExecutor(Task task, Procedure procedure, Job job) {
		super(classId);
		this.task = task;
		this.procedure = procedure;
		this.job = job;
		if (job == null) { // was called from TaskUpdate
			this.isExecutor = false;
		} else { // Was called from MessageConsumer
			this.isExecutor = true;
		}
	}

	public static JobCalculationExecutor create(Task task, Procedure procedure, Job job) {
		return new JobCalculationExecutor(task, procedure, job);
	}

	@Override
	public void run() {
		if (isExecutor) {
			executeTask(task, procedure, job);
		} else {
			switchDataFromTaskToProcedureDomain(procedure, task);
		}
	}

	public void executeTask(Task task, Procedure procedure, Job job) {
		if (task.canBeExecuted() && !task.isInProcedureDomain() && canExecute) {

			logger.info("executeTask: Task to execute: " + task);
			FutureGet getAllFuture = dhtConnectionProvider.getAll(task.key(), procedure.dataInputDomain().toString()).addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						if (canExecute) {
							List<Object> values = syncedArrayList();
							Set<Number640> valueSet = future.dataMap().keySet();
							for (Number640 valueHash : valueSet) {
								Object taskValue = ((Value) future.dataMap().get(valueHash).object()).value();
								values.add(taskValue);
							}

							JobProcedureDomain outputJPD = JobProcedureDomain.create(procedure.jobId(), procedure.dataInputDomain().jobSubmissionCount(), id,
									procedure.executable().getClass().getSimpleName(), procedure.procedureIndex(), procedure.currentExecutionNumber());
							synchronized (locks) {
								Boolean lock = locks.get(outputJPD);
								if (lock == null) {
									lock = false;
									locks.put(outputJPD, lock);
								}
							}

							ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.currentExecutionNumber(), outputJPD);

							DHTStorageContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
							DHTStorageContext contextToUse = context; // Afterwards, will be checked if combiner was used and if so, will be replaced with the combiner context
							if (procedure.combiner() != null) {
								DHTStorageContext combinerContext = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
								context.combiner((IExecutable) procedure.combiner(), combinerContext);
							}

							if (canExecute) {
								((IExecutable) procedure.executable()).process(task.key(), values, context);
								futuresForTask.putAll(task, context.futurePutData()); // Could be empty if combiner is used --> no effect
								if (procedure.combiner() != null) {
									context.combine();
									futuresForTask.putAll(task, context.combinerContext().futurePutData());
									contextToUse = context.combinerContext();
								}
								outputETD.resultHash(contextToUse.resultHash());
								if (contextToUse.futurePutData().size() > 0) {
									FutureDone<List<FuturePut>> futureDone = Futures.whenAllSuccess(contextToUse.futurePutData());
									futuresForTask.put(task, futureDone);
									futureDone.addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
										@Override
										public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
											if (future.isSuccess()) {
												broadcastTaskCompletion(task, procedure, outputETD, job);
											} else {
												logger.warn("executeTask:: No success on task[" + task.key() + "] execution. Reason: " + future.failedReason());
											}
										}

									});
								} else {// FuturePut data is 0 --> may happen if the Executable does not produce any results (e.g. only words with count > 1 are emitted)
									broadcastTaskCompletion(task, procedure, outputETD, job);
								}
							}
						}
					} else {
						logger.info(
								"Could not retrieve data for task " + task.key() + " in job procedure domain: " + procedure.dataInputDomain().toString() + ". Failed reason: " + future.failedReason());
					}
				}

			});
			futuresForTask.put(task, getAllFuture);
		}
	}

	public void abortExecution() {
		canExecute = false;
		logger.info("Aborting Task: " + task.key() + ", futuresForTask.keySet().size(): " + futuresForTask.keySet().size() + " [" + futuresForTask.keySet() + "], " + futuresForTask.values().size()
				+ "[" + futuresForTask.values() + "]");

		synchronized (futuresForTask) {
			for (Task task : futuresForTask.keySet()) {
				Collection<BaseFuture> futures = futuresForTask.get(task);
				for (BaseFuture b : futures) {
					b.cancel();
				}
				// logger.info("Aborted [" + futures.size() + "] remaining futures of task [" + task.key() + "]");
			}
		}
	}

	private void broadcastTaskCompletion(Task task, Procedure procedure, ExecutorTaskDomain outputETD, Job job) {

		if (canExecute) {
			synchronized (locks.get(outputETD.jobProcedureDomain())) { // TODO: check if that lock works correctly in case of different JPDs (the only reason to have a lock is to not lock on the
																		// intermediate map because else every other
				// executor gets blocked here... this way, only the correct executors get blocked that executed on the same JPD...
				if (!locks.get(outputETD.jobProcedureDomain())) {
					locks.put(outputETD.jobProcedureDomain(), true);
					logger.info("Executor for task [" + task.key() + "] ACQUIRED lock [" + locks.get(outputETD.jobProcedureDomain()) + "]");
					List<ExecutorTaskDomain> etds = intermediate.get(outputETD.jobProcedureDomain());

					Integer submittedTaskCount = submitted.get(outputETD.jobProcedureDomain());
					if (submittedTaskCount == null) {
						submittedTaskCount = 0;
					}
					if ((etds.size() == ((int) (procedure.tasksSize() * procedure.taskSummarisationFactor()))) || (submittedTaskCount == numberOfExecutions - 1)) {
						etds.add(outputETD);
						// TODO: or message would get bigger than 9000bytes (BC limit). AND: how to know when all task executions finished ? What if tasks are executed multiple
						// times?(submittedTaskCount)
						// --> it does not matter if it will finish in the end. If another executor finishes first, these executions will simply be aborted and ignored...
						CompletedTaskBCMessage msg = CompletedTaskBCMessage.create(outputETD.jobProcedureDomain(),
								procedure.dataInputDomain().nrOfFinishedTasks(procedure.nrOfFinishedAndTransferredTasks()));

						for (ExecutorTaskDomain etd : etds) {
							msg.addOutputDomainTriple(etd);
						}
						// submittedTaskCount += etds.size();
						submitted.put(outputETD.jobProcedureDomain(), submittedTaskCount);
						etds.clear();
						if (canExecute) {
							dhtConnectionProvider.broadcastCompletion(msg);
							dhtConnectionProvider.broadcastHandler().processMessage(msg, job);
							logger.info("executeTask: Successfully broadcasted CompletedTaskMessage for task " + msg);
						}
					} else {
						etds.add(outputETD);
					}
					submitted.put(outputETD.jobProcedureDomain(), ++submittedTaskCount);
					locks.put(outputETD.jobProcedureDomain(), false);
					logger.info("Executor for task [" + task.key() + "] RELEASED lock [" + locks.get(outputETD.jobProcedureDomain()) + "]");
				}
			}
		}

	}

	public void switchDataFromTaskToProcedureDomain(Procedure procedure, Task task) {
		if (task.isFinished() && !task.isInProcedureDomain() && canExecute) {
			// logger.info("switchDataFromTaskToProcedureDomain: Transferring task " + task + " to procedure domain ");
			List<FutureGet> futureGetKeys = syncedArrayList();
			List<FutureGet> futureGetValues = syncedArrayList();
			List<FuturePut> futurePutKeys = syncedArrayList();
			List<FuturePut> futurePutValues = syncedArrayList();

			ExecutorTaskDomain fromETD = task.resultOutputDomain();

			JobProcedureDomain toJPD = JobProcedureDomain.create(procedure.jobId(), procedure.dataInputDomain().jobSubmissionCount(), id, procedure.executable().getClass().getSimpleName(),
					procedure.procedureIndex(), procedure.currentExecutionNumber());
			FutureGet getAllTaskResultKeys = dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, fromETD.toString());
			futureGetKeys.add(getAllTaskResultKeys);// for inside method completion
			futuresForTask.put(task, getAllTaskResultKeys);// for outside abortion

			getAllTaskResultKeys.addListener(new BaseFutureAdapter<FutureGet>() {

				@Override
				public void operationComplete(FutureGet future) throws Exception {
					if (future.isSuccess()) {
						if (canExecute) {
							Set<Number640> keySet = future.dataMap().keySet();
							for (Number640 n : keySet) {
								String taskOutputKey = (String) future.dataMap().get(n).object();
								// logger.info("transferDataFromETDtoJPD:: taskOutputKey: " + taskOutputKey);
								FutureGet getAllTaskValues = dhtConnectionProvider.getAll(taskOutputKey, fromETD.toString());
								futureGetValues.add(getAllTaskValues); // For inside method completion
								futuresForTask.put(task, getAllTaskResultKeys); // For outside abortion
								getAllTaskValues.addListener(new BaseFutureAdapter<FutureGet>() {

									@Override
									public void operationComplete(FutureGet future) throws Exception {
										if (future.isSuccess()) {
											if (canExecute) {
												FuturePut putValuesForTaskProcedure = dhtConnectionProvider.addAll(taskOutputKey, future.dataMap().values(), toJPD.toString());
												futurePutValues.add(putValuesForTaskProcedure); // For inside method completion
												futuresForTask.put(task, putValuesForTaskProcedure); // For outside abortion

												putValuesForTaskProcedure.addListener(new BaseFutureAdapter<FuturePut>() {

													@Override
													public void operationComplete(FuturePut future) throws Exception {

														if (future.isSuccess()) {
															if (canExecute) {
																logger.info("transferDataFromETDtoJPD::Successfully added task output values of task output key \"" + taskOutputKey + "\" for task "
																		+ task.key() + " to output procedure domain " + toJPD.toString());

																FuturePut putTaskForProcedure = dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskOutputKey, toJPD.toString(),
																		false);
																futurePutKeys.add(putTaskForProcedure); // For inside method completion
																futuresForTask.put(task, putTaskForProcedure); // For outside abortion

																putTaskForProcedure.addListener(new BaseFutureAdapter<FuturePut>() {

																	@Override
																	public void operationComplete(FuturePut future) throws Exception {
																		if (future.isSuccess()) {
																			logger.info("transferDataFromETDtoJPD::Successfully transferred task output key \"" + taskOutputKey + "\" for task "
																					+ task.key() + " from task output domain " + fromETD.toString() + " to procedure output domain "
																					+ toJPD.toString());
																		} else {
																			logger.info("transferDataFromETDtoJPD::Failed to add task output key and values for task output key \"" + taskOutputKey
																					+ "\" for task " + task.key() + " to output procedure domain " + toJPD.toString() + ", failed reason: "
																					+ future.failedReason());
																		}
																	}

																});
															}
														} else {
															logger.info("transferDataFromETDtoJPD::Failed to add values for task output key " + taskOutputKey + " to output procedure domain "
																	+ toJPD.toString() + ", failed reason: " + future.failedReason());
														}
													}

												});
											}

										} else {
											logger.info("transferDataFromETDtoJPD::Failed to get task output key and values for task output key (" + taskOutputKey + " from task executor domain "
													+ fromETD.toString() + ", failed reason: " + future.failedReason());

										}
									}
								});

							}
						}
					} else {
						logger.warn("transferDataFromETDtoJPD::Failed to get task keys for task " + task.key() + " from task executor domain " + fromETD.toString() + ", failed reason: "
								+ future.failedReason());
					}
				}
			});
			// logger.info("switchDataFromTaskToProcedureDomain:: futureGetKeys.size(): " + futureGetKeys.size());

			FutureDone<List<FutureGet>> whenAllFutureGetKeysSuccess = Futures.whenAllSuccess(futureGetKeys);
			futuresForTask.put(task, whenAllFutureGetKeysSuccess); // For outside abortion

			if (canExecute) {
				whenAllFutureGetKeysSuccess.addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

					@Override
					public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
						if (future.isSuccess()) {
							if (canExecute) {

								// logger.info("switchDataFromTaskToProcedureDomain::futureGetValues.size(): " + futureGetValues.size());
								if (futureGetValues.size() > 0) {
									FutureDone<List<FutureGet>> whenAllFutureGetValuesSuccess = Futures.whenAllSuccess(futureGetValues);
									futuresForTask.put(task, whenAllFutureGetValuesSuccess); // For outside abortion
									whenAllFutureGetValuesSuccess.addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

										@Override
										public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
											if (future.isSuccess()) {

												if (canExecute) {
													// logger.info("switchDataFromTaskToProcedureDomain::futurePuts.size(): " + futurePuts.size());
													FutureDone<List<FuturePut>> whenAllFuturePutValuesSuccess = Futures.whenAllSuccess(futurePutValues);
													futuresForTask.put(task, whenAllFutureGetValuesSuccess); // For outside abortion
													whenAllFuturePutValuesSuccess.addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

														@Override
														public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
															if (future.isSuccess()) {
																if (canExecute) {
																	FutureDone<List<FuturePut>> whenAllFuturePutKeysSuccess = Futures.whenAllSuccess(futurePutKeys);
																	futuresForTask.put(task, whenAllFuturePutKeysSuccess);
																	whenAllFuturePutKeysSuccess.addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

																		@Override
																		public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
																			if (future.isSuccess()) {
																				broadcastProcedureCompleted(procedure, task, toJPD);
																			} else {
																				logger.warn("switchDataFromTaskToProcedureDomain:: Failed to transfered task output keys and values for task " + task
																						+ " from task executor domain to job procedure domain: " + toJPD.toString() + ". failed reason: "
																						+ future.failedReason());
																			}
																		}
																	});
																}
															} else {
																logger.warn("switchDataFromTaskToProcedureDomain:: Failed to transfered task output keys and values for task " + task
																		+ " from task executor domain to job procedure domain: " + toJPD.toString() + ". failed reason: " + future.failedReason());
															}
														}

													});
												}
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
							}
						} else {
							logger.warn("switchDataFromTaskToProcedureDomain::Failed to get task keys for task " + task + " from task executor domain. failed reason: " + future.failedReason());
						}
					}

				});
			}
		}
	}

	private void broadcastProcedureCompleted(Procedure procedure, Task taskToTransfer, JobProcedureDomain to) {
		if (canExecute) {
			taskToTransfer.isInProcedureDomain(true);
			logger.info("broadcastProcedureCompleted:: Successfully transfered task output keys and values for task " + taskToTransfer + " from task executor domain to job procedure domain: "
					+ to.toString() + ". ");

			CompletedProcedureBCMessage msg = tryCompletingProcedure(procedure);
			if (msg != null) {
				if (canExecute) {
					dhtConnectionProvider.broadcastCompletion(msg);
					dhtConnectionProvider.broadcastHandler().processMessage(msg, dhtConnectionProvider.broadcastHandler().getJob(procedure.jobId()));
					logger.info("broadcastProcedureCompleted:: Broadcasted Completed Procedure MSG: " + msg);
				}
			}
		}
	}

	public static CompletedProcedureBCMessage tryCompletingProcedure(Procedure procedure) {
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		int expectedSize = dataInputDomain.expectedNrOfFiles();
		int currentSize = procedure.tasksSize();

		// logger.info("tryCompletingProcedure: data input domain procedure: " + dataInputDomain.procedureSimpleName()+", all tasks in procedure: " + procedure.tasks());
		if (expectedSize == currentSize) {
			logger.info("tryCompletingProcedure: expectedSize == currentSize  " + expectedSize + "==" + currentSize);

			if (procedure.isCompleted()) {
				logger.info("tryCompletingProcedure:procedure.isCompleted()  " + procedure.isCompleted());
				JobProcedureDomain outputProcedure = JobProcedureDomain.create(procedure.jobId(), dataInputDomain.jobSubmissionCount(), JobCalculationExecutor.classId,
						procedure.executable().getClass().getSimpleName(), procedure.procedureIndex(), procedure.currentExecutionNumber()).resultHash(procedure.resultHash())
						.expectedNrOfFiles(currentSize);
				logger.info("tryCompletingProcedure::Resetting procedure");
				procedure.reset();// Is finished, don't need the tasks anymore...
				CompletedProcedureBCMessage msg = CompletedProcedureBCMessage.create(outputProcedure, dataInputDomain);
				return msg;
			} else {
				logger.info("tryCompletingProcedure: !procedure.isCompleted()  ");
			}
		} else {
			logger.info("tryCompletingProcedure: expectedSize != currentSize  " + expectedSize + "!=" + currentSize);

		}
		return null;
	}

	@Override
	public JobCalculationExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}
	//
	// @Override
	// public JobCalculationExecutor performanceInformation(PerformanceInfo performanceInformation) {
	// this.performanceInformation = performanceInformation;
	// return this;
	// }

	public JobCalculationExecutor numberOfExecutions(int numberOfExecutions) {
		this.numberOfExecutions = numberOfExecutions;
		return this;
	}

	public void clearMaps(JobProcedureDomain domainToClear) {
		intermediate.removeAll(domainToClear);
		submitted.remove(domainToClear);
	}

}
