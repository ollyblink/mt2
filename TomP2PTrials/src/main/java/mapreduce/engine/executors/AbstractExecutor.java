package mapreduce.engine.executors;

import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.PerformanceInfo;

public abstract class AbstractExecutor implements IExecutor {
	protected IDHTConnectionProvider dhtConnectionProvider;

	protected AbstractExecutor() {

	}

}
