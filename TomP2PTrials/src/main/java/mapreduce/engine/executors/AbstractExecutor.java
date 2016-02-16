package mapreduce.engine.executors;

import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.IDCreator;

public abstract class AbstractExecutor implements IExecutor {
	protected IDHTConnectionProvider dhtConnectionProvider;

	protected AbstractExecutor() {

	}

}
