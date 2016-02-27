package net.tomp2p.mapreduce.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Map;

public class FileObjectInputStream extends ObjectInputStream {
	private Map<String, Class<?>> classes;

	public FileObjectInputStream(InputStream in, Map<String, Class<?>> classes) throws IOException {
		super(in);
		this.classes = classes;
	}

	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
		Class<?> c = classes.get(desc.getName());
		if (c != null) {
			return c;
		}
		return super.resolveClass(desc); // To change body of generated methods,
											// choose Tools | Templates.
	}

}