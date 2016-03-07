package net.tomp2p.mapreduce;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import mapreduce.storage.DHTWrapper;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;

public class FileSplitterTest {

	@Test
	public void test() {
		// Put data
		String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
		DHTWrapper dht = Mockito.mock(DHTWrapper.class);
		List<Number160> fileKeys = Collections.synchronizedList(new ArrayList<>());
		List<FutureTask> filePuts = Collections.synchronizedList(new ArrayList<>());

		List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
		FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
		assertEquals(3, pathVisitor.size());

		for (String filePath : pathVisitor) {
			Map<Number160, FutureTask> tmp = FileSplitter.splitWithWordsAndWrite(filePath, dht, 3, Number160.createHash("DOMAINKEY"), FileSize.MEGA_BYTE.value(), "UTF-8");
			assertEquals(1, tmp.keySet().size());
			fileKeys.addAll(tmp.keySet());
			filePuts.addAll(tmp.values());
		}
		assertEquals(3, fileKeys.size());
		assertEquals(3, filePuts.size());
	}

}
