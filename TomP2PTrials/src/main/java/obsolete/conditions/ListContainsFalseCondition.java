package obsolete.conditions;

import java.util.List;

public class ListContainsFalseCondition implements ICondition<List<Boolean>> {

	@Override
	public boolean metBy(List<Boolean> t) {
		return t.contains(false);
	}

	public static ListContainsFalseCondition create() {
		return new ListContainsFalseCondition();
	}

	private ListContainsFalseCondition() {

	}
}