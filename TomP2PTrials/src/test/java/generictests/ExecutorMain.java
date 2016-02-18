package generictests;

import java.util.Random;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;

public class ExecutorMain {
	private static Random random = new Random();

	public static void main(String[] args) throws Exception {

		// JobCalculationExecutor calculationExecutor = JobCalculationExecutor.create();

		// JobCalculationMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create(4);
		// JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create(1).messageConsumer(calculationMessageConsumer);

		IDHTConnectionProvider dhtCon = null;
		// dhtCon = TestUtils.getTestConnectionProvider(executorBCHandler);
		// Thread.sleep(1000);
		// DHTConnectionProvider
		// .create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
		// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
		;
		// f
		JobCalculationMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create(4);
		JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create().messageConsumer(calculationMessageConsumer);
		// IDHTConnectionProvider dhtCon = null;
		if (Integer.parseInt(args[1]) == 1) {// Bootstrapper
			dhtCon = DHTConnectionProvider.create("192.168.43.65", Integer.parseInt(args[0]), Integer.parseInt(args[0])).broadcastHandler(executorBCHandler)
			// .storageFilePath(System.getProperty("user.dir")
			//
			// +
			// "/src/test/java/mapreduce/engine/componenttests/storage/calculator/")
			;
		} else {
			int other = random.nextInt(40000) + 4000;
			dhtCon = DHTConnectionProvider.create("192.168.43.65", Integer.parseInt(args[0]), other).broadcastHandler(executorBCHandler)// .storageFilePath(System.getProperty("user.dir")
			//
			// +
			// "/src/test/java/mapreduce/engine/componenttests/storage/calculator/")
			;
		}

		// dhtCon.broadcastHandler(executorBCHandler);
		// calculationExecutor.dhtConnectionProvider(dhtCon);
		// calculationMessageConsumer.dhtConnectionProvider(dhtCon);
		dhtCon.broadcastHandler(executorBCHandler).connect();
		// calculationExecutor.dhtConnectionProvider(dhtCon);
		calculationMessageConsumer.dhtConnectionProvider(dhtCon);

		while (executorBCHandler.jobFutures().isEmpty()) {
			Thread.sleep(10);
		}
		Job job = executorBCHandler.jobFutures().keySet().iterator().next();
		while (!job.isFinished()) {
			Thread.sleep(2000);
		}
		System.err.println("Shutting down executor in 15 seconds");
		Thread.sleep(15000);
		System.err.println("Shutting down executor");
		executorBCHandler.shutdown();
		dhtCon.shutdown();
		Thread.sleep(5000);
		// System.exit(0);
	}
	/*
	 * 12:29:32.121 [NETTY-TOMP2P - worker-client/server - -1-7] ERROR io.netty.util.ResourceLeakDetector - LEAK: AlternativeCompositeByteBuf.release() was not called before it's garbage-collected.
	 * Enable advanced leak reporting to find out where the leak occurred. To enable advanced leak reporting, specify the JVM option '-Dio.netty.leakDetectionLevel=advanced' or call
	 * ResourceLeakDetector.setLevel() See http://netty.io/wiki/reference-counted-objects.html for more information.
	 * 
	 */
}
