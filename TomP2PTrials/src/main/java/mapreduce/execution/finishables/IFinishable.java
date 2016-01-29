package mapreduce.execution.finishables;

import java.io.Serializable;

import mapreduce.execution.domains.IDomain;
import net.tomp2p.peers.Number160;

/**
 * All finishable classes (Job, Procedure, Task) need this
 * 
 * @author Oliver
 *
 */
public interface IFinishable extends Serializable {
	public boolean isFinished();

	public IFinishable needsMultipleDifferentExecutors(boolean needsMultipleDifferentDomains);

	public IFinishable addOutputDomain(IDomain domain);

	public Number160 resultHash();

	public IDomain resultOutputDomain();

	public int nrOfOutputDomains();

	public IFinishable nrOfSameResultHash(int nrOfSameResultHash);

	public Integer currentMaxNrOfSameResultHash();

	public void reset();

	public boolean needsMultipleDifferentExecutors();

	public int nrOfSameResultHash();
 

}
