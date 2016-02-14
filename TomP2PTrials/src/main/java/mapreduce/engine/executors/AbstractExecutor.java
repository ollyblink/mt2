package mapreduce.engine.executors;

import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.storage.IDHTConnectionProvider;

public abstract class AbstractExecutor implements IExecutor  {

	protected IDHTConnectionProvider dhtConnectionProvider;
	protected String id;
	// Static because its gonna be the same on every computer anyways...
	protected static PerformanceInfo performanceInformation = PerformanceInfo.create();

	protected AbstractExecutor(String id) {
		this.id = id;
	}

	@Override
	public String id() {
		return id;
	}

	@Override
	public PerformanceInfo performanceInformation() {
		return performanceInformation;
	}

}
