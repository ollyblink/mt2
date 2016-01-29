package mapreduce.engine.messageconsumers;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
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

	private Map<String, Boolean> currentlyRetrievingTaskKeysForProcedure = SyncedCollectionProvider
			.syncedHashMap();

	private Map<String, ListMultimap<Task, Future<?>>> futures = SyncedCollectionProvider.syncedHashMap();

	private Comparator<PerformanceInfo> performanceEvaluator = new Comparator<PerformanceInfo>() {

		@Override
		public int compare(PerformanceInfo o1, PerformanceInfo o2) {
			// TODO: finished the same number of tasks with different data sets... What could it be? E.g.
			// compare processor capabilities and take
			// the one with the better ones as the faster will most likely finish more tasks quicker. Or
			// compare the tasks output values size
			return 1;
		}

	};

	private IResultPrinter resultPrinter = DefaultResultPrinter.create();

	private JobCalculationMessageConsumer(int maxThreads) {
		super();
		this.threadPoolExecutor = PriorityExecutor.newFixedThreadPool(maxThreads);

	}

	public static JobCalculationMessageConsumer create(int nrOfThreads) {
		return new JobCalculationMessageConsumer(nrOfThreads);
	}

	public static JobCalculationMessageConsumer create() {
		return new JobCalculationMessageConsumer(DEFAULT_NR_OF_THREADS);
	}

	@Override
	public void handleCompletedProcedure(Job job, JobProcedureDomain outputDomain,
			JobProcedureDomain inputDomain) {
		logger.info("handleCompletedProcedure:: entered");
		handleReceivedMessage(job, outputDomain, inputDomain, new ProcedureUpdate(job, this));
	}

	@Override
	public void handleCompletedTask(Job job, ExecutorTaskDomain outputDomain,
			JobProcedureDomain inputDomain) {
		logger.info("handleCompletedTask:: entered");
		handleReceivedMessage(job, outputDomain, inputDomain, new TaskUpdate(this));
	}

	private void handleReceivedMessage(Job job, IDomain outputDomain, JobProcedureDomain inputDomain,
			IUpdate iUpdate) {
		logger.info("handleReceivedMessage:: entered");
		if (job == null || outputDomain == null || inputDomain == null || iUpdate == null) {

			logger.info("handleReceivedMessage:: input was null: job: " + job + ", outputDomain: "
					+ outputDomain + ", inputDomain:" + inputDomain + ", iUpdate: " + iUpdate);
			return;
		}
		JobProcedureDomain rJPD = (outputDomain instanceof JobProcedureDomain
				? (JobProcedureDomain) outputDomain
				: ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());

		boolean receivedOutdatedMessage = job.currentProcedure().procedureIndex() > rJPD.procedureIndex();
		logger.info("handleReceivedMessage:: before if/else, receivedOutdatedMessage: "
				+ receivedOutdatedMessage + ", received JPD: " + rJPD);
		if (receivedOutdatedMessage) {
			logger.info("handleReceivedMessage:: Received an old message: nothing to do.");
			return;
		} else {
			// need to increment procedure because we are behind in execution?
			logger.info("handleReceivedMessage:: before tryIncrementProcedure");
			tryIncrementProcedure(job, inputDomain, rJPD);
			// Same input data? Then we may try to update tasks/procedures
			logger.info("handleReceivedMessage:: before tryUpdateTasksOrProcedure");
			tryUpdateTasksOrProcedures(job, inputDomain, outputDomain, iUpdate);
			// Anything left to execute for this procedure?
			logger.info("handleReceivedMessage:: before tryExecuteProcedure");
			tryExecuteProcedure(job);
		}
		logger.info("handleReceivedMessage:: done");
	}

	private void tryIncrementProcedure(Job job, JobProcedureDomain dataInputDomain, JobProcedureDomain rJPD) {
		logger.info("tryIncrementProcedure:: entered");
		int currentProcIndex = job.currentProcedure().procedureIndex();
		int receivedPIndex = rJPD.procedureIndex();
		logger.info("tryIncrementProcedure:: currentProcIndex < receivedPIndex? "
				+ (currentProcIndex < receivedPIndex));
		if (currentProcIndex < receivedPIndex) {
			// Means this executor is behind in the execution than the one that sent this message -->
			// increment until we are up to date again
			logger.info("tryIncrementProcedure:: before cancel execution on data input domain: "
					+ job.currentProcedure().dataInputDomain().procedureSimpleName().toString());
			cancelProcedureExecution(job.currentProcedure().dataInputDomain().toString());
			logger.info("tryIncrementProcedure:: before incrementing procedure index from "
					+ job.currentProcedure().procedureIndex() + " to " + receivedPIndex);
			while (job.currentProcedure().procedureIndex() < receivedPIndex) {
				job.incrementProcedureIndex();
			}

			logger.info("tryIncrementProcedure:: updated procedure index to "
					+ job.currentProcedure().procedureIndex());
		} // no else needed... if it's the same procedure index, we are up to date and can try to update
		logger.info("tryIncrementProcedure:: job.currentProcedure().dataInputDomain() == null? "
				+ (job.currentProcedure().dataInputDomain() == null));

		if (job.currentProcedure().dataInputDomain() == null) { // may happen in case StartProcedure tasks are
																// received

			job.currentProcedure().dataInputDomain(dataInputDomain);
			logger.info("tryIncrementProcedure:: updated input domain for proc: "
					+ job.currentProcedure().executable().getClass().getSimpleName() + " from null to "
					+ dataInputDomain);
		}
		logger.info("tryIncrementProcedure:: done");

	}

	private void tryUpdateTasksOrProcedures(Job job, JobProcedureDomain inputDomain, IDomain outputDomain,
			IUpdate iUpdate) {
		Procedure procedure = job.currentProcedure();
		logger.info("tryUpdateTasksOrProcedures::procedure? "
				+ procedure.executable().getClass().getSimpleName());

		logger.info("tryUpdateTasksOrProcedures::procedure.dataInputDomain().equals(inputDomain)?: "
				+ procedure.dataInputDomain().equals(inputDomain));
		if (procedure.dataInputDomain().equals(inputDomain)) { // same procedure, same input data location:
																// everything is fine!
			logger.info(
					"tryUpdateTasksOrProcedures::procedure.dataInputDomain().expectedNrOfFiles() < inputDomain.expectedNrOfFiles()?: "
							+ procedure.dataInputDomain().expectedNrOfFiles() + " < "
							+ inputDomain.expectedNrOfFiles() + ": " + (procedure.dataInputDomain()
									.expectedNrOfFiles() < inputDomain.expectedNrOfFiles()));
			if (procedure.dataInputDomain().expectedNrOfFiles() < inputDomain.expectedNrOfFiles()) {// looks
																									// like
																									// the
																									// received
																									// had
																									// more
																									// already
				procedure.dataInputDomain().expectedNrOfFiles(inputDomain.expectedNrOfFiles());
				logger.info("tryUpdateTasksOrProcedures:: updated nr of expected files to "
						+ inputDomain.expectedNrOfFiles());
			}
			logger.info("tryUpdateTasksOrProcedures:: inputDomain.isJobFinished()? "
					+ inputDomain.isJobFinished());
			if (inputDomain.isJobFinished()) {
				logger.info("tryUpdateTasksOrProcedures:: before cancelProcedureExecution("
						+ procedure.dataInputDomain().procedureSimpleName() + ");");

				cancelProcedureExecution(procedure.dataInputDomain().toString());
			} else { // Only here: execute the received task/procedure update
				logger.info("tryUpdateTasksOrProcedures:: before iUpdate.executeUpdate(" + outputDomain + ", "
						+ procedure.executable().getClass().getSimpleName() + ");");
				iUpdate.executeUpdate(outputDomain, procedure);
			}
		} else { // May have to change input data location (inputDomain)
			// executor of received message executes on different input data! Need to synchronize
			logger.info(
					"tryUpdateTasksOrProcedures::else if(procedure.dataInputDomain().equals(inputDomain)){before changeDataInputDomain("
							+ inputDomain + "," + procedure.executable().getClass().getSimpleName() + ")}");

			changeDataInputDomain(inputDomain, procedure);
		}
	}

	private void changeDataInputDomain(JobProcedureDomain inputDomain, Procedure procedure) {
		if (procedure.dataInputDomain().nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {
			// We have completed fewer tasks with our data set than the incoming... abort us and use the
			// incoming data set location instead
			cancelCurrentExecutionsAndUpdateDataInputDomain(inputDomain, procedure);
		} else {
			boolean haveExecutedTheSameNrOfTasks = procedure.dataInputDomain()
					.nrOfFinishedTasks() == inputDomain.nrOfFinishedTasks();
			if (haveExecutedTheSameNrOfTasks) {
				int comparisonResult = this.performanceEvaluator.compare(executor.performanceInformation(),
						inputDomain.executorPerformanceInformation());
				boolean thisExecutorHasWorsePerformance = comparisonResult == -1;
				if (thisExecutorHasWorsePerformance) {
					// we are expected to finish later due to worse performance --> abort this one's execution
					cancelCurrentExecutionsAndUpdateDataInputDomain(inputDomain, procedure);
				}
			}
		} // else{ ignore, as we are the ones that finished more already...
	}

	private void cancelCurrentExecutionsAndUpdateDataInputDomain(JobProcedureDomain inputDomain,
			Procedure procedure) {
		cancelProcedureExecution(procedure.dataInputDomain().toString());
		procedure.dataInputDomain(inputDomain);
	}

	private void tryExecuteProcedure(Job job) {
		Procedure procedure = job.currentProcedure();
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		boolean isJobFinished = job.isFinished();
		logger.info("tryExecuteProcedure:: job.isFinished()? " + isJobFinished);
		if (isJobFinished) {
			dataInputDomain.isJobFinished(true);
			CompletedBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(dataInputDomain,
					dataInputDomain);
			dhtConnectionProvider.broadcastCompletion(msg);
			logger.info(
					"tryExecuteProcedure:: final data domain to retrieve results from: " + dataInputDomain);
			resultPrinter.printResults(dhtConnectionProvider, dataInputDomain.toString());

		} else {//
			boolean isProcedureFinished = procedure.isFinished();
			logger.info(
					"tryExecuteProcedure:: is procedure [" + procedure.executable().getClass().getSimpleName()
							+ "] finished? " + isProcedureFinished);
			if (!isProcedureFinished) {
				boolean isNotComplete = procedure.tasksSize() < dataInputDomain.expectedNrOfFiles();
				boolean isNotStartProcedure = procedure.procedureIndex() > 0;

				logger.info("tryExecuteProcedure::isNotComplete? " + isNotComplete);
				logger.info("tryExecuteProcedure::isNotStartProcedure? " + isNotStartProcedure);
				if (isNotComplete && isNotStartProcedure) {
					logger.info("tryExecuteProcedure:: before tryRetrieveMoreTasksFromDHT("
							+ procedure.executable().getClass().getSimpleName() + ")");
					tryRetrieveMoreTasksFromDHT(procedure);
				}
				boolean isExpectedToBeComplete = procedure.tasksSize() == dataInputDomain.expectedNrOfFiles();

				logger.info("tryExecuteProcedure:: isExpectedToBeComplete? " + procedure.tasksSize() + " == "
						+ dataInputDomain.expectedNrOfFiles() + "? " + isExpectedToBeComplete);
				if (isExpectedToBeComplete) {
					logger.info("tryExecuteProcedure:: before trySubmitTasks("
							+ procedure.executable().getClass().getSimpleName() + ")");
					trySubmitTasks(procedure);
				}
			}
		}
	}

	private void tryRetrieveMoreTasksFromDHT(Procedure procedure) {
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		Boolean retrieving = currentlyRetrievingTaskKeysForProcedure.get(dataInputDomain.toString());
		if ((retrieving == null || !retrieving)) { // This makes sure that if it is concurrently executed,
													// retrieval is only once called...
			logger.info("tryRetrieveMoreTasksFromDHT::Retrieving tasks for: " + dataInputDomain.toString());
			currentlyRetrievingTaskKeysForProcedure.put(dataInputDomain.toString(), true);
			dhtConnectionProvider
					.getAll(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, dataInputDomain.toString())
					.awaitUninterruptibly() //TODO remove?
					.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								int actualNrOfTasks = future.dataMap().size();
								logger.info("tryRetrieveMoreTasksFromDHT::retrieved " + actualNrOfTasks
										+ " tasks from dataInputDomain: " + dataInputDomain.toString());
								dataInputDomain.expectedNrOfFiles(actualNrOfTasks);
								for (Number640 keyHash : future.dataMap().keySet()) {
									String key = (String) future.dataMap().get(keyHash).object();
									Task task = Task.create(key, executor.id());
									procedure.addTask(task);
								}
								currentlyRetrievingTaskKeysForProcedure.remove(dataInputDomain.toString());
							} else {
								logger.info("Fail reason: " + future.failedReason());
							}
						}
					});
		}
	}

	private void trySubmitTasks(Procedure procedure) {
		logger.info("trySubmitTasks:: Shuffle tasks: " + procedure.tasksSize());
		procedure.shuffleTasks();
		Task task = null;
		while ((task = procedure.nextExecutableTask()) != null) {
			logger.info("trySubmitTasks::Next task: " + task.key() + ", is finished? " + task.isFinished());
			if (!task.isFinished()) {
				logger.info("trySubmitTasks:: next task to add to thread pool executor: " + task.key());
				Runnable runnable = createTaskExecutionRunnable(procedure, task);
				Future<?> futureTaskExecution = threadPoolExecutor.submit(runnable, task);
				addTaskFuture(procedure.dataInputDomain().toString(), task, futureTaskExecution);
			}
		}
	}

	private Runnable createTaskExecutionRunnable(Procedure procedure, Task task) {
		logger.info("createTaskExecutionRunnable:: create executor().executeTask(" + task.key() + ", "
				+ procedure.executable().getClass().getSimpleName() + ")");
		return new Runnable() {
			@Override
			public void run() {
				executor().executeTask(task, procedure);
			}
		};
	}

	private void addTaskFuture(String dataInputDomainString, Task task, Future<?> taskFuture) {
		ListMultimap<Task, Future<?>> taskFutures = futures.get(dataInputDomainString);
		if (taskFutures == null) {
			taskFutures = SyncedCollectionProvider.syncedArrayListMultimap();
			futures.put(dataInputDomainString, taskFutures);
		}
		taskFutures.put(task, taskFuture);
		logger.info("added task future to taskFutures map:taskFutures.put(" + task.key() + ", " + taskFuture
				+ ");");
	}

	public void cancelProcedureExecution(String dataInputDomainString) {
		ListMultimap<Task, Future<?>> procedureFutures = futures.get(dataInputDomainString);
		if (procedureFutures != null) {
			for (Future<?> taskFuture : procedureFutures.values()) {
				taskFuture.cancel(true);
			}
			procedureFutures.clear();
		}
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

	public JobCalculationMessageConsumer performanceEvaluator(
			Comparator<PerformanceInfo> performanceEvaluator) {
		this.performanceEvaluator = performanceEvaluator;
		return this;
	}
}
