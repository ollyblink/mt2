package mapreduce.execution.finishables;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.peers.Number160;

public abstract class AbstractFinishable implements IFinishable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8676046636876323261L;

	private static Logger logger = LoggerFactory.getLogger(AbstractFinishable.class);

	/** final output domain for where this tasks output key/values are stored */
	protected IDomain resultOutputDomain;
	/** Domain (ExecutorTaskDomains) of keys for next procedure */
	protected List<IDomain> outputDomains;
	/** How many times this object needs to be executed before it is declared finished */
	protected int nrOfSameResultHash = 0;
	/** Assert that there are multiple output domains received before a IFinishable is finished */
	protected boolean needsMultipleDifferentExecutors;

	public AbstractFinishable() {
		this.outputDomains = SyncedCollectionProvider.syncedArrayList();
	}

	@Override
	public void reset() {
		resultOutputDomain = null;
		outputDomains.clear();
	}

	@Override
	public boolean isFinished() {
		if (nrOfSameResultHash > 0) {
			checkIfFinished();
			boolean isFinished = resultOutputDomain != null;
			return isFinished;
		} else {  
			return true;
		}
	}

	@Override
	// Always takes the first one!
	public IDomain resultOutputDomain() {
		checkIfFinished();
		return resultOutputDomain;
	}

	protected boolean containsExecutor(String localExecutorId) {
		for (IDomain domain : outputDomains) {
			if (domain.executor().equals(localExecutorId)) {
				return true;
			}
		}
		return false;
	}

	protected void checkIfFinished() {
		ListMultimap<Number160, IDomain> results = SyncedCollectionProvider.syncedArrayListMultimap();
		for (IDomain domain : outputDomains) {
			results.put(domain.resultHash(), domain);
		}
		boolean isFinished = false;
		Number160 r = null;
		if (currentMaxNrOfSameResultHash() >= nrOfSameResultHash) {
			synchronized (results) {
				for (Number160 resultHash : results.keySet()) {
					if (resultHash == null) {
						break;
					} else if (results.get(resultHash).size() >= nrOfSameResultHash) {
						if (needsMultipleDifferentExecutors) {

							List<IDomain> list = results.get(resultHash);
							Set<String> asSet = new HashSet<>();

							for (IDomain d : list) {
								asSet.add(d.executor());
							}
							if (asSet.size() < nrOfSameResultHash) {
								continue;
							}
						}
						r = resultHash;
						isFinished = true;
						break;
					}
				}
			}
		}
		if (isFinished) {
			// It doesn't matter which one...
			this.resultOutputDomain = results.get(r).get(0);
		} else {
			this.resultOutputDomain = null;
		}
	}

	@Override
	public int nrOfOutputDomains() {
		return outputDomains.size();
	}

	@Override
	public Integer currentMaxNrOfSameResultHash() {
		ListMultimap<Number160, IDomain> results = ArrayListMultimap.create();
		for (IDomain domain : outputDomains) {
			if (domain.resultHash() != null) {
				results.put(domain.resultHash(), domain);
			}
		}
		TreeSet<Integer> max = new TreeSet<>();
		for (Number160 resultHash : results.keySet()) {
			max.add(results.get(resultHash).size());
		}
		if (max.size() > 0) {
			return max.last();
		} else {
			return 0;
		}
	}

	@Override
	public AbstractFinishable nrOfSameResultHash(int nrOfSameResultHash) {
		this.nrOfSameResultHash = nrOfSameResultHash;
		return this;
	}

	@Override
	public AbstractFinishable addOutputDomain(IDomain domain) {
		logger.info("addOutputDomain:: try adding [" + domain + "]");
		if (!this.outputDomains.contains(domain)) {
			logger.info("addOutputDomain:: this.outputDomains.contains(domain)? [false]");
			if (!isFinished()) {
				logger.info("addOutputDomain:: this.isFinished()? [false]");
				if (needsMultipleDifferentExecutors) {
					logger.info("addOutputDomain:: this.needsMultipleDifferentExecutors()? [true]");
					if (!containsExecutor(domain.executor())) {
						logger.info(
								"addOutputDomain:: this.containsExecutor(domain.executor())? [false] --> added domain ["
										+ domain + "]");
						this.outputDomains.add(domain);
					}
				} else {
					logger.info(
							"addOutputDomain:: this.needsMultipleDifferentExecutors()? [false] --> added domain ["
									+ domain + "]");
					this.outputDomains.add(domain);
				}
			}
		}
		return this;
	}

	@Override
	public int nrOfSameResultHash() {
		return nrOfSameResultHash;
	}

	@Override
	public boolean needsMultipleDifferentExecutors() {
		return needsMultipleDifferentExecutors;
	}

	@Override
	public AbstractFinishable needsMultipleDifferentExecutors(boolean needsMultipleDifferentExecutors) {
		this.needsMultipleDifferentExecutors = needsMultipleDifferentExecutors;
		return this;
	}

	@Override
	public String toString() {
		return "AbstractFinishable [resultOutputDomain=" + resultOutputDomain + ", outputDomains="
				+ outputDomains + ", nrOfSameResultHash=" + nrOfSameResultHash + ", isFinished()="
				+ isFinished() + "]";
	}

}