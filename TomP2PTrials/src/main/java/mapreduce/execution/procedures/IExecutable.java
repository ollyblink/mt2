package mapreduce.execution.procedures;

import java.io.Serializable;
import java.util.Collection;

import mapreduce.execution.context.IContext;

public interface IExecutable extends Serializable {
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context);

}
