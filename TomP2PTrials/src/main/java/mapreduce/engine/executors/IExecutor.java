package mapreduce.engine.executors;

import mapreduce.storage.IDHTConnectionProvider;

public interface IExecutor {

	public IExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

	// public IExecutor performanceInformation(PerformanceInfo performanceInformation);

}
