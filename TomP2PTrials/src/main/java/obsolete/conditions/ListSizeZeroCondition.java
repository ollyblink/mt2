package obsolete.conditions;

import java.util.List;

public class ListSizeZeroCondition<T> implements ICondition<List<T>> {

	private ListSizeZeroCondition() {

	}

	@Override
	public boolean metBy(List<T> t) {
		return (t.size() == 0);
	}

	public static <T> ListSizeZeroCondition<T> create() {
		return new ListSizeZeroCondition<>();
	}
}