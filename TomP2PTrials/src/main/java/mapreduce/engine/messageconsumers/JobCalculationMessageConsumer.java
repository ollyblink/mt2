package mapreduce.engine.messageconsumers;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.CompletedProcedureBCMessage;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.engine.messageconsumers.updates.IUpdate;
import mapreduce.engine.messageconsumers.updates.ProcedureUpdate;
import mapreduce.engine.messageconsumers.updates.TaskUpdate;
import mapreduce.engine.multithreading.PriorityExecutor;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.resultprinter.DefaultResultPrinter;
import mapreduce.utils.resultprinter.IResultPrinter;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;

public class JobCalculationMessageConsumer extends AbstractMessageConsumer {
	private static final int DEFAULT_NR_OF_THREADS = 1;

	private static Logger logger = LoggerFactory.getLogger(JobCalculationMessageConsumer.class);

	private PriorityExecutor threadPoolExecutor;

	private Map<String, Boolean> currentlyRetrievingTaskKeysForProcedure = SyncedCollectionProvider.syncedHashMap();

	private Map<String, ListMultimap<Task, Future<?>>> futures = SyncedCollectionProvider.syncedHashMap();

	// private Comparator<PerformanceInfo> performanceEvaluator = new Comparator<PerformanceInfo>() {
	//
	// @Override
	// public int compare(PerformanceInfo o1, PerformanceInfo o2) {
	// return o1.compareTo(o2);
	// }
	//
	// };

	private IResultPrinter resultPrinter = DefaultResultPrinter.create();

	private int maxThreads;

 
	private JobCalculationMessageConsumer(int maxThreads) {
		this.maxThreads = maxThreads;
		this.threadPoolExecutor = PriorityExecutor.newFixedThreadPool(maxThreads);

	}

	public static JobCalculationMessageConsumer create(int nrOfThreads) {
		return new JobCalculationMessageConsumer(nrOfThreads);
	}

	public static JobCalculationMessageConsumer create() {
		return new JobCalculationMessageConsumer(DEFAULT_NR_OF_THREADS);
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomain, inputDomain, ProcedureUpdate.create(job, this, outputDomain));
	}

	@Override
	public void handleCompletedTask(Job job, List<ExecutorTaskDomain> outputDomains, JobProcedureDomain inputDomain) {
		handleReceivedMessage(job, outputDomains.get(0), inputDomain, TaskUpdate.create(this, outputDomains));
	}

	private void handleReceivedMessage(Job job, IDomain outputDomain, JobProcedureDomain inputDomain, IUpdate iUpdate) {
		// logger.info("handleReceivedMessage:: entered");
		// if (job == null || outputDomain == null || inputDomain == null || iUpdate == null) {
		// logger.info("handleReceivedMessage:: input was null: job: " + job + ", outputDomain: " + outputDomain + ", inputDomain:" + inputDomain + ", iUpdate: " + iUpdate);
		// return;
		// }
		JobProcedureDomain rJPD = (outputDomain instanceof JobProcedureDomain ? (JobProcedureDomain) outputDomain : ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());

		// boolean receivedOutdatedMessage = job.currentProcedure().procedureIndex() > rJPD.procedureIndex();
		// if (receivedOutdatedMessage) {
		// logger.info("handleReceivedMessage:: I (" + executor.id() + ") Received an old message: nothing to do. message contained rJPD:" + rJPD + " but I already use procedure "
		// + job.currentProcedure().procedureIndex());
		// return;
		// } else {
		// need to increment procedure because we are behind in execution?
		logger.info("handleReceivedMessage, before tryIncrementProcedure:need to increment procedure because we are behind in execution?");
		tryIncrementProcedure(job, inputDomain, rJPD);
		// Same input data? Then we may try to update tasks/procedures
		logger.info("handleReceivedMessage, before tryUpdateTasksOrProcedures: Same input data? Then we may try to update tasks/procedures");
		tryUpdateTasksOrProcedures(job, inputDomain, outputDomain, iUpdate);
		// Anything left to execute for this procedure?
		logger.info("handleReceivedMessage, before evaluateJobFinished: Anything left to execute for this procedure?");
		evaluateJobFinished(job);
		// }
	}

	private void tryIncrementProcedure(Job job, JobProcedureDomain dataInputDomain, JobProcedureDomain rJPD) {

		int currentProcIndex = job.currentProcedure().procedureIndex();
		int receivedPIndex = rJPD.procedureIndex();
		logger.info("tryIncrementProcedure:: currentProcIndex: " + currentProcIndex + ", receivedPIndex: " + receivedPIndex + ", currentProcIndex < receivedPIndex? "
				+ (currentProcIndex < receivedPIndex));
		if (currentProcIndex < receivedPIndex) {
			logger.info(
					"tryIncrementProcedure:: currentProcIndex < receivedPIndex: Means this executor is behind in the execution than the one that sent this message --> increment until we are up to date again");
			// Means this executor is behind in the execution than the one that sent this message --> increment until we are up to date again
			cancelProcedureExecution(job.currentProcedure().dataInputDomain().toString());
			while (job.currentProcedure().procedureIndex() < receivedPIndex) {
				job.incrementProcedureIndex();
			}
		} // no else needed... if our's is the same or larger procedure index, we are up to date and can try to update
		else {
			logger.info("tryIncrementProcedure:: currentProcIndex >= receivedPIndex? (" + currentProcIndex + " >= " + receivedPIndex
					+ "): no else needed... if our's is the same or larger procedure index, we are up to date and can try to update");
		}

	}

	private void tryUpdateTasksOrProcedures(Job job, JobProcedureDomain inputDomain, IDomain outputDomain, IUpdate iUpdate) {
		Procedure procedure = job.currentProcedure();
		if (procedure.dataInputDomain() == null) {// TODO: added this here from tryIncrementProcedure as it fits better here --> TEST
			logger.info(
					"tryUpdateTasksOrProcedures:: job.currentProcedure().dataInputDomain() == null:  may happen in case StartProcedure tasks are received or when the procedure index is incremented--> currentProcedure is: "
							+ procedure.executable().getClass().getSimpleName() + ", dataInputDomain.procName: " + inputDomain.procedureSimpleName());
			// may happen in case StartProcedure tasks are received or when the procedure index is incremented
			procedure.dataInputDomain(inputDomain);
		} else {
			logger.info("tryUpdateTasksOrProcedures:: job.currentProcedure().dataInputDomain() != null:: Check if the input data is the same as the received one. if not, we may have to change it");
			if (!procedure.dataInputDomain().equals(inputDomain)) { // TODO TEST IF THAT IS CORRECT?
				// TODO: rethink: shouldn't it here also execute the update in case we were the ones that did not finish correctly? then we shouldn't just ignore the update...
				// executor of received message executes on different input data! Need to synchronize
				logger.info("tryUpdateTasksOrProcedures:: not the same inputdomain (!procedure.dataInputDomain().equals(inputDomain)): changeDataInputDomain is called");
				changeDataInputDomain(inputDomain, procedure);
			} else {
				logger.info("tryUpdateTasksOrProcedures:: same inputdomain (procedure.dataInputDomain().equals(inputDomain)): ignore this attempt of changing the input domain");
			}
		}
		if (procedure.dataInputDomain().expectedNrOfFiles() < inputDomain.expectedNrOfFiles()) {// looks like the received had more already
			logger.info("tryUpdateTaskOrProcedures:: procedure.dataInputDomain().expectedNrOfFiles() < inputDomain.expectedNrOfFiles() is true: updating expected nr of files from "
					+ procedure.dataInputDomain().expectedNrOfFiles() + " to " + inputDomain.expectedNrOfFiles());
			procedure.dataInputDomain().expectedNrOfFiles(inputDomain.expectedNrOfFiles());
		} else {
			logger.info("procedure.dataInputDomain().expectedNrOfFiles() >= inputDomain.expectedNrOfFiles()" + procedure.dataInputDomain().expectedNrOfFiles() + " >= "
					+ inputDomain.expectedNrOfFiles() + ", the received input domain had less expected files... nothing to update...");
		}

		if (inputDomain.isJobFinished()) {
			logger.info("tryUpdateTasksOrProcedures::inputDomain.isJobFinished() is true --> if there is any execution of this job left, abort it!");
			cancelProcedureExecution(procedure.dataInputDomain().toString());
		} else { // Only here: execute the received task/procedure update
			logger.info("tryUpdateTasksOrProcedures::inputDomain.isJobFinished() is false --> finally execute the update.");
			if (executor().id().equals(inputDomain.executor())) {
				logger.info("tryUpdateTasksOrProcedures:: My data is used as input! " + inputDomain + ", output domain:" + outputDomain);
			} else {
				logger.info("tryUpdateTasksOrProcedures::  " + inputDomain.executor() + "'s data is used as input! " + inputDomain + ", output domain:" + outputDomain);
			}
			iUpdate.executeUpdate(procedure);
		}

	}

	private void changeDataInputDomain(JobProcedureDomain inputDomain, Procedure procedure) {
		if (procedure.dataInputDomain().nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {
			// We have completed fewer tasks with our data set than the incoming... abort us and use the incoming data set location instead
			logger.info("changeDataInputDomain::ABORTING TASK EXECUTION!: procedure.dataInputDomain().nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks(): "
					+ procedure.dataInputDomain().nrOfFinishedTasks() + "< " + inputDomain.nrOfFinishedTasks());
			cancelProcedureExecution(procedure.dataInputDomain().toString());
			procedure.dataInputDomain(inputDomain);
		} else if (procedure.dataInputDomain().nrOfFinishedTasks() == inputDomain.nrOfFinishedTasks()) {
			logger.info(
					"changeDataInputDomain::We executed the same number of tasks as the executor we received the data from... we only abort execution if we are worse than the other in performance (or random number)");
			int comparisonResult = performanceInformation.compareTo(inputDomain.executorPerformanceInformation());
			boolean thisExecutorHasWorsePerformance = comparisonResult == 1; // smaller value means better (smaller execution time)
			logger.info("changeDataInputDomain::comparisonResult is " + comparisonResult + "(which means thisExecutorHasWorsePerformance is " + thisExecutorHasWorsePerformance);
			if (thisExecutorHasWorsePerformance) {
				// we are expected to finish later due to worse performance --> abort this one's execution
				logger.info("changeDataInputDomain::thisExecutorHasWorsePerformance is true, we are worse! ABORTING TASK EXECUTION!");
				cancelProcedureExecution(procedure.dataInputDomain().toString());
				procedure.dataInputDomain(inputDomain);
			} else {
				logger.info("changeDataInputDomain::thisExecutorHasWorsePerformance is false, we are better! Task execution on this input data is not aborted!");

			}
		} else { // ignore, as we are the ones that finished more already...
			logger.info("changeDataInputDomain:: We already finished more! " + procedure.dataInputDomain().nrOfFinishedTasks() + " vs. received " + inputDomain.nrOfFinishedTasks()
					+ " --> ignore the data input domain change.");
		}
	}

	private void evaluateJobFinished(Job job) {
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		if (job.isFinished()) {
			dataInputDomain.isJobFinished(true);
			CompletedProcedureBCMessage msg = CompletedProcedureBCMessage.create(dataInputDomain, dataInputDomain);
			dhtConnectionProvider.broadcastCompletion(msg);
			logger.info("evaluateJobFinished::Job is finished... set isJobFinished to true and broadcast the final CompletedProcedureBCMessage. Print results to the screen if needed. BC Message was: "
					+ msg);
			resultPrinter.printResults(dhtConnectionProvider, dataInputDomain.toString());
		} else {//
			logger.info("evaluateJobFinished::Job is NOT finished... check if the procedure is finished.");
			boolean isProcedureFinished = procedure.isFinished();
			if (!isProcedureFinished) {
				logger.info("evaluateJobFinished:: Procedure is NOT finished... check if procedure is complete (all tasks received) and it is not the StartProcedure... ");
				boolean isNotComplete = procedure.tasksSize() < dataInputDomain.expectedNrOfFiles();
				boolean isNotStartProcedure = procedure.procedureIndex() > 0;
				if (isNotComplete && isNotStartProcedure) {
					logger.info("evaluateJobFinished::There are tasks left to be retrieved and it is not the StartProcedure: retrieve data from the DHT and start the corresponding tasks");
					tryRetrieveMoreTasksFromDHT(procedure);
				}
			} else {

				logger.info("evaluateJobFinished::Procedure is finished...SHOULD NOT BE REACHED ACTUALLY..Nothing to do here apparently.");
			}
		}
	}

	private void tryRetrieveMoreTasksFromDHT(Procedure procedure) {
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		Boolean retrieving = currentlyRetrievingTaskKeysForProcedure.get(dataInputDomain.toString());
		if ((retrieving == null || !retrieving)) { // This makes sure that if it is concurrently executed, retrieval is only once called...
			currentlyRetrievingTaskKeysForProcedure.put(dataInputDomain.toString(), true);
			dhtConnectionProvider.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataInputDomain.toString()).awaitUninterruptibly() // TODO remove?
					.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								int actualNrOfTasks = future.dataMap().size();
								dataInputDomain.expectedNrOfFiles(actualNrOfTasks);
								for (Number640 keyHash : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyHash).object();
									Task task = Task.create(key, executor.id());
									procedure.addTask(task);
								}
								currentlyRetrievingTaskKeysForProcedure.remove(dataInputDomain.toString());
								boolean isExpectedToBeComplete = procedure.tasksSize() == dataInputDomain.expectedNrOfFiles();
								if (isExpectedToBeComplete) {
									trySubmitTasks(procedure);
								}
							} else {
								logger.info("Fail reason: " + future.failedReason());
							}
						}
					});
		}
	}

	private void trySubmitTasks(Procedure procedure) {
		procedure.shuffleTasks(); // Avoid executing tasks in the same order!
		Task task = null;
		while ((task = procedure.nextExecutableTask()) != null) {
			executor().numberOfExecutions(procedure.numberOfExecutions());// needs to be updated every time as execution may already start...

			// if (!task.isFinished()) {
			final Task taskToExecute = task; // that final stuff is annoying...
			// Create the future execution of this task}
			Future<?> taskFuture = threadPoolExecutor.submit(new Runnable() {
				@Override
				public void run() {
					executor().executeTask(taskToExecute, procedure, dhtConnectionProvider.broadcastHandler().getJob(procedure.jobId()));
				}
			}, task);
			// Add it to the futures for possible later abortion if needed.
			ListMultimap<Task, Future<?>> taskFutures = futures.get(procedure.dataInputDomain().toString());
			if (taskFutures == null) {
				taskFutures = SyncedCollectionProvider.syncedArrayListMultimap();
				futures.put(procedure.dataInputDomain().toString(), taskFutures);
			}
			taskFutures.put(task, taskFuture);
			// logger.info("trySubmitTasks::added task future to taskFutures map:taskFutures.put(" + task.key() + ", " + taskFuture + ");");
			// }
		}
	}

	public void cancelProcedureExecution(String dataInputDomainString) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(dataInputDomainString);
		if (procedureFutures != null) {
			for (Future<?> taskFuture : procedureFutures.values()) {
				taskFuture.cancel(true);
			}
			procedureFutures.clear();
		}
		threadPoolExecutor.shutdown();
		// Wait for everything to finish.
		try {
			while (!threadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
				logger.info("Awaiting completion of threads.");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// threadPoolExecutor.shutdownNow();
		this.threadPoolExecutor = PriorityExecutor.newFixedThreadPool(maxThreads);
	}

	public void cancelTaskExecution(String dataInputDomainString, Task task) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(dataInputDomainString);
		if (procedureFutures != null) {
			List<Future<?>> taskFutures = procedureFutures.get(task);
			for (Future<?> taskFuture : taskFutures) {
				taskFuture.cancel(true);
			}
			procedureFutures.get(task).clear();
		}
	}

	@Override
	public JobCalculationExecutor executor() {
		return (JobCalculationExecutor) super.executor();
	}

	@Override
	public JobCalculationMessageConsumer dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		return (JobCalculationMessageConsumer) super.dhtConnectionProvider(dhtConnectionProvider);
	}

	@Override
	public JobCalculationMessageConsumer executor(IExecutor executor) {
		return (JobCalculationMessageConsumer) super.executor(executor);
	}

	public JobCalculationMessageConsumer resultPrinter(IResultPrinter resultPrinter) {
		this.resultPrinter = resultPrinter;
		return this;
	}

	// public JobCalculationMessageConsumer performanceEvaluator(Comparator<PerformanceInfo> performanceEvaluator) {
	// this.performanceEvaluator = performanceEvaluator;
	// return this;
	// }
}
