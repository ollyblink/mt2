package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.junit.Test;

import net.tomp2p.mapreduce.utils.SerializeUtils;

public class SerializeUtilsTest {
	
	private class InnerTestClass implements Serializable {

	}

	private static class InnerStaticTestClass implements Serializable {

	}

	private interface InnerTestInterface extends Serializable {

	}

	private static class TestClass implements Serializable {
		private class InnerTestClass implements Serializable {

		}

		private static class InnerStaticTestClass implements Serializable {

		}

		private interface InnerTestInterface extends Serializable {

		}
	}

	@Test
	public void testSerialize() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(TestClass.class);
		assertEquals(4, serialize.keySet().size());
		assertEquals(true, serialize.keySet().contains(TestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerTestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerStaticTestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerTestInterface.class.getName()));
	}
	@Test
	public void testDeserialize() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(TestClass.class);
		TestClass testClass = (TestClass)SerializeUtils.deserialize(serialize, TestClass.class.getName());
		 
	}
}
