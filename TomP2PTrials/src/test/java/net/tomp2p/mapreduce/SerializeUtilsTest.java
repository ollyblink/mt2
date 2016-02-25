package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import junit.framework.TestCase;
import net.tomp2p.mapreduce.utils.SerializeUtils;

public class SerializeUtilsTest {
	public static class TestClass implements Serializable {
		Runnable r = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}
		};

		private class InnerTestClass implements Serializable {
			Runnable r = new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub

				}
			};

			public void print() {
				System.out.println("Hello InnerTestClass");
				
			}
		}

		private static class InnerStaticTestClass implements Serializable {

		}

		private interface InnerTestInterface extends Serializable {

		}

		public class AnonoymousContainers {
			Runnable r = new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					Runnable r = new Runnable() {

						@Override
						public void run() {
							// TODO Auto-generated method stub
							Runnable r = new Runnable() {

								@Override
								public void run() {
									// TODO Auto-generated method stub
									Runnable r = new Runnable() {

										@Override
										public void run() {
											// TODO Auto-generated method stub

										}
									};
									Runnable r2 = new Runnable() {

										@Override
										public void run() {
											// TODO Auto-generated method stub

										}
									};
									Runnable r3 = new Runnable() {

										@Override
										public void run() {
											// TODO Auto-generated method stub

										}
									};
								}
							};
						}
					};
				}
			};
			Runnable r2 = new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					Runnable r = new Runnable() {

						@Override
						public void run() {
							// TODO Auto-generated method stub

						}
					};
				}
			};
			Runnable r3 = new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub

				}
			};
		}

		public void print() {
			System.out.println("Hello World");
		}
	}

	class InnerTestClass implements Serializable {

	}

	private static class InnerStaticTestClass implements Serializable {

	}

	private interface InnerTestInterface extends Serializable {

	}

	@Test
	public void testSerializeSinglePrivateInnerTestClass() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(InnerTestClass.class);
		assertEquals(1, serialize.keySet().size());
		assertEquals(true, serialize.keySet().contains(InnerTestClass.class.getName()));
	}

	@Test
	public void testSerializeSinglePrivateStaticInnerTestClass() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(InnerStaticTestClass.class);
		assertEquals(1, serialize.keySet().size());
		assertEquals(true, serialize.keySet().contains(InnerStaticTestClass.class.getName()));
	}

	@Test
	public void testSerializeSingleInterface() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(InnerTestInterface.class);
		assertEquals(1, serialize.keySet().size());
		assertEquals(true, serialize.keySet().contains(InnerTestInterface.class.getName()));
	}

	@Test
	public void testSerializeExternalDeclaredAndAnonymousInnerClasses() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(TestClass.class);
		for (String name : serialize.keySet()) {
			FileOutputStream output = new FileOutputStream(new File(name + ".class"));
			output.write(serialize.get(name));
			output.close();
		}

		assertEquals(true, serialize.keySet().contains(TestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.class.getName() + "$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerTestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerTestClass.class.getName() + "$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerStaticTestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.InnerTestInterface.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName()));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$1$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$1$1$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$1$1$1$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$1$1$1$3"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$2"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$2$1"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$1$1$1$2"));
		assertEquals(true, serialize.keySet().contains(TestClass.AnonoymousContainers.class.getName() + "$3"));
		assertEquals(16, serialize.keySet().size());
	}
	
	@Test
	public void testSerializeInternalExternalDeclaredAndAnonymousInnerClasses() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(SerializeUtilsTest.class);
		for (String name : serialize.keySet()) {
			FileOutputStream output = new FileOutputStream(new File(name + ".class"));
			output.write(serialize.get(name));
			output.close();
		}

		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.class.getName() + "$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.InnerTestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.InnerTestClass.class.getName() + "$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.InnerStaticTestClass.class.getName()));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.InnerTestInterface.class.getName()));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName()));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$1$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$1$1$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$1$1$1$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$1$1$1$3"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$2"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$2$1"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$1$1$1$2"));
		assertEquals(true, serialize.keySet().contains(SerializeUtilsTest.TestClass.AnonoymousContainers.class.getName() + "$3"));
		assertEquals(17, serialize.keySet().size());
	}

	// public static void main(String[] args) throws IOException {
	// Map<String, byte[]> serialize =
	// SerializeUtils.serialize(TestClass.class);
	// for (String name : serialize.keySet()) {
	// FileOutputStream output = new FileOutputStream(new
	// File("/home/ozihler/git/mt2/TomP2PTrials/src/test/java/net/tomp2p/mapreduce/testclassfiles/"+name+".class"));
	// output.write(serialize.get(name));
	// output.close();
	// }
	//
	// }
	@Test
	public void testDeserialize() throws IOException {
		Map<String, byte[]> serialize = SerializeUtils.serialize(SerializeUtilsTest.class);
		// Write files to file system
		for (String name : serialize.keySet()) {
			FileOutputStream output = new FileOutputStream(
					new File("/home/ozihler/git/mt2/TomP2PTrials/src/test/java/net/tomp2p/mapreduce/testclassfiles/"
							+ name + ".class"));
			output.write(serialize.get(name));
			output.close();
		}
		Map<String, byte[]> toDeserialize = new HashMap<>();
		String toInstantiate = "";
		for (String name : serialize.keySet()) {
			if (name.endsWith("net.tomp2p.mapreduce.SerializeUtilsTest$TestClass$InnerTestClass")) {
				toInstantiate = name;
			}
			Path path = Paths
					.get("/home/ozihler/git/mt2/TomP2PTrials/src/test/java/net/tomp2p/mapreduce/testclassfiles/" + name
							+ ".class");
			byte[] data = Files.readAllBytes(path);
			toDeserialize.put(name, data);
		}
		SerializeUtilsTest.TestClass.InnerTestClass deserialize = (SerializeUtilsTest.TestClass.InnerTestClass) SerializeUtils.deserialize(toDeserialize, toInstantiate);
		deserialize.print();
	}

}
