package mapreduce.utils.resultprinter;

import mapreduce.storage.IDHTConnectionProvider;

public interface IResultPrinter {

	public void printResults(IDHTConnectionProvider dhtConnectionProvider, String domainString);

}
