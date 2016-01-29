package mapreduce.execution.context;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class DHTStorageContextTest {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContextTest.class);
	private String executor = "EXECUTOR_1";

	@Test
	public void test() throws InterruptedException {
		IDHTConnectionProvider dhtConnectionProvider = TestUtils.getTestConnectionProvider(4000, 1);

		Job job = Job.create("SUBMITTER_1", PriorityLevel.MODERATE);
		Task task = Task.create("hello", executor);
		JobProcedureDomain outputJPD = JobProcedureDomain.create(job.id(), 0, executor, "NONE", 0);
		ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), executor, task.newStatusIndex(), outputJPD);
		IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);

		for (int i = 0; i < 10; ++i) {
			context.write(task.key(), new Integer(1));
		}
		List<Object> values = SyncedCollectionProvider.syncedArrayList();
		Futures.whenAllSuccess(context.futurePutData()).awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {

			@Override
			public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
				if (future.isSuccess()) {
					dhtConnectionProvider.getAll("hello", outputETD.toString()).awaitUninterruptibly()
							.addListener(new BaseFutureAdapter<FutureGet>() {

						@Override
						public void operationComplete(FutureGet future) throws Exception {
							if (future.isSuccess()) {
								logger.info("future is success");
								if (future.dataMap() != null) {
									for (Number640 n : future.dataMap().keySet()) {
										Object value = ((Value) future.dataMap().get(n).object()).value();
										values.add(value);
									}
								}
							} else {
								logger.warn("No success on retrieving values for 'hello'");
							}
						}

					});
				} else {
					logger.warn("No success on putting data");
				}
			}
		});
		assertEquals(10, values.size());
		for (int i = 0; i < 10; ++i) {
			assertEquals(true, values.get(i) instanceof Integer);
			assertEquals(new Integer(1), ((Integer) values.get(i)));
		}

	}

}
