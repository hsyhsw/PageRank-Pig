package kr.ac.kaist.adward.pagerankpig.util;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Created by adward on 4/14/14.
 */
public class TreeMultiMap<K extends Comparable<K>, V> {

	private TreeMap<K, List<V>> map;

	private int size;

	public TreeMultiMap() {
		map = new TreeMap<K, List<V>>();
		size = 0;
	}

	public V put(K key, V value) {
		List<V> vals = null;

		if (map.containsKey(key)) {
			vals = map.get(key);
		} else {
			vals = new ArrayList<V>();
			map.put(key, vals);
		}

		boolean added = vals.add(value);
		V addedVal = null;
		if (added) {
			++size;
			addedVal = value;
		}

		return addedVal;
	}

	public V remove(K key) {
		List<V> vals = map.get(key);
		V removed = null;

		if (vals != null) {
			--size;
			removed = vals.remove(0);
			if (vals.size() == 0) {
				map.remove(key);
			}
		}

		return removed;
	}

	public K firstKey() {
		return map.firstKey();
	}

	public Multimap<K, V> descendingMap() {
		ImmutableMultimap.Builder<K, V> builder = new ImmutableMultimap.Builder<K, V>();
		builder.orderKeysBy(new Comparator<K>() { // descending multimap.
			@Override
			public int compare(K k, K k2) {
				return k2.compareTo(k);
			}
		});

		NavigableMap<K, List<V>> descKeyListMap = map.descendingMap();
		for (Map.Entry<K, List<V>> entry : descKeyListMap.entrySet()) {
			builder.putAll(entry.getKey(), entry.getValue());
		}

		ImmutableMultimap<K, V> descMap = builder.build();

		return descMap;
	}

	public Collection<V> values() {
		List<V> vals = new ArrayList<V>();

		for (Collection<V> vs : map.values())
			vals.addAll(vs);

		return vals;
	}

	public int size() {
		return size;
	}

}
