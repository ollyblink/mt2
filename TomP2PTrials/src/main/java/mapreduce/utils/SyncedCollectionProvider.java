package mapreduce.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 * I don't want to worry about always declaring them synchronized... so I simply create one of these
 * 
 * @author Oliver
 *
 */
public class SyncedCollectionProvider {
	public static <T> Set<T> syncedHashSet() {
		return Collections.synchronizedSet(new HashSet<T>());
	}

	public static <T> SortedSet<T> syncedTreeSet() {
		return Collections.synchronizedSortedSet(new TreeSet<T>());
	}

	public static <T> List<T> syncedArrayList() {
		return Collections.synchronizedList(new ArrayList<T>());
	}

	public static <T> List<T> syncedLinkedList() {
		return Collections.synchronizedList(new LinkedList<T>());
	}

	public static <K, V> Map<K, V> syncedHashMap() {
		return Collections.synchronizedMap(new HashMap<K, V>());
	}

	public static <K, V> SortedMap<K, V> syncedTreeMap() {
		return Collections.synchronizedSortedMap(new TreeMap<K, V>());
	}

	/**
	 * I love multimaps... probably the best invention ever
	 * 
	 * @return
	 */
	public static <K, V> ListMultimap<K, V> syncedArrayListMultimap() {
		return Multimaps.synchronizedListMultimap(ArrayListMultimap.create());

	}
}
