package mapreduce.execution.context;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ListMultimap;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number160;

public class DHTStorageContext implements IContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);
	// private IExecutable combiner;
	private Number160 resultHash = Number160.ZERO;
	private IDHTConnectionProvider dhtConnectionProvider;
	private List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
	private ExecutorTaskDomain oETD;
	private IExecutable combiner;
	private ListMultimap<Object, Object> valuesForCombiner;
	private DHTStorageContext combinerContext;

	/**
	 * 
	 * @param dhtConnectionProvider
	 * @param taskResultComparator
	 *            may add certain speed ups such that the task result comparison afterwards becomes faster
	 */
	private DHTStorageContext() {
	}

	public static DHTStorageContext create() {
		return new DHTStorageContext();
	}

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (combiner == null) { // normal case
			writeToDHT(keyOut, valueOut);
		} else {
			valuesForCombiner.put(keyOut, valueOut);
		}
	}

	private void writeToDHT(Object keyOut, Object valueOut) {
		if (combiner != null) {
			logger.info("Combiner:" + combiner.getClass().getSimpleName());
		}
		updateResultHash(keyOut, valueOut);
		this.futurePutData.add(add(keyOut.toString(), valueOut, oETD.toString(), true));
		this.futurePutData.add(add(DomainProvider.TASK_OUTPUT_RESULT_KEYS, keyOut.toString(), oETD.toString(), false));

	}

	private FuturePut add(String keyOut, Object valueOut, String oETDString, boolean asList) {
		return this.dhtConnectionProvider.add(keyOut, valueOut, oETDString, asList).addListener(new BaseFutureAdapter<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("add::SUCCESS::" + oETD.jobProcedureDomain().procedureSimpleName() + ":(" + keyOut + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
				} else {
					logger.info("add::FAILED::" + oETD.jobProcedureDomain().procedureSimpleName() + ":(" + keyOut + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
				}
			}
		});
	}

	public DHTStorageContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public Number160 resultHash() {
		return this.resultHash;
	}

	public List<FuturePut> futurePutData() {
		return this.futurePutData;
	}

	public DHTStorageContext outputExecutorTaskDomain(ExecutorTaskDomain outputExecutorTaskDomain) {
		this.oETD = outputExecutorTaskDomain;
		return this;
	}

	public DHTStorageContext combiner(IExecutable combiner, DHTStorageContext combinerContext) {
		this.combiner = combiner;
		this.combinerContext = combinerContext;
		this.valuesForCombiner = SyncedCollectionProvider.syncedArrayListMultimap();
		return this;
	}

	private void updateResultHash(Object keyOut, Object valueOut) {
		resultHash = resultHash.xor(Number160.createHash(keyOut.toString())).xor(Number160.createHash(valueOut.toString()));
	}

	public void combine() {
		if (combiner != null && combinerContext != null) {
			for (Object key : valuesForCombiner.keySet()) {
				combiner.process(key, valuesForCombiner.get(key), combinerContext);
			}
		}
	}

	public DHTStorageContext combinerContext() {
		return combinerContext;
	}

}
