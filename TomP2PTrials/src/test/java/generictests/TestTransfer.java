package generictests;

import java.util.HashMap;
import java.util.Set;

import javax.print.attribute.HashAttributeSet;

public class TestTransfer {
	public static void main(String[] args) {
		HashMap<String, HashMap<String, Integer>> dht = new HashMap<>();

		HashMap<String, Integer> map = new HashMap<>();
		map.put("Hello", 1);
		map.put("world", 1);
		dht.put("ETD", map);
		Set<String> keySet = dht.get("ETD").keySet();
		for(String key: keySet){
//			dht.putAll(key, );
		}
	}
}
