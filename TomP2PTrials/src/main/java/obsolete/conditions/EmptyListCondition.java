package obsolete.conditions;

import java.util.List;

public class EmptyListCondition<T> implements ICondition<List<T>> {

	private EmptyListCondition() {

	}

	public static <T> EmptyListCondition<T> create() {
		return new EmptyListCondition<>();
	}

	@Override
	public boolean metBy(List<T> t) {
		return t.isEmpty();
	}
}