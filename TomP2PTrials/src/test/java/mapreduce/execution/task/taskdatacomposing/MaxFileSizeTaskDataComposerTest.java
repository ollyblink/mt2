package mapreduce.execution.task.taskdatacomposing;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;

import mapreduce.execution.tasks.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.utils.FileSize;

public class MaxFileSizeTaskDataComposerTest {

	@Test
	public void testSplit() {
		MaxFileSizeTaskDataComposer instance = MaxFileSizeTaskDataComposer.create();
		instance.maxFileSize(FileSize.EIGHT_BYTES);
		instance.fileEncoding("UTF-8");
		instance.splitValue(" ");
		String[] results = { "this", "is a", "test", "this", "is a", "test" };

		String toSplit = "this is a test this is a test";
		List<String> splits = instance.splitToSize(toSplit);
		for (int i = 0; i < splits.size(); ++i) {
			assertEquals(results[i], splits.get(i));
			assertEquals(true, splits.get(i).getBytes(Charset.forName(instance.fileEncoding())).length <= instance.maxFileSize().value());
			String concat = splits.get(i) + " " + results[i + 1];
			assertEquals(true, concat.getBytes(Charset.forName(instance.fileEncoding())).length > instance.maxFileSize().value());
		}
		assertEquals(results[results.length - 1], instance.remainingData());
		assertEquals(true, results[results.length - 1].getBytes(Charset.forName(instance.fileEncoding())).length <= instance.maxFileSize().value());

	}

}
