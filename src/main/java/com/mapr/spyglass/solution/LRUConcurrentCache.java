package com.mapr.spyglass.solution;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;

public class LRUConcurrentCache<K,V> {   
	
	private final Map<K,V> cache;   
	private static final Logger log = Logger.getLogger(LRUConcurrentCache.class);
	
	public LRUConcurrentCache(final int maxEntries) {
		this.cache = new LinkedHashMap<K,V>(maxEntries, 0.75F, true) {
			private static final long serialVersionUID = -1236481390177598762L;
			@Override
			protected boolean removeEldestEntry(Map.Entry<K,V> eldest){
				if (size() > maxEntries) log.info("Removing oldest entry");
				return size() > maxEntries;
			}
		};
	}

	public int size() {
		synchronized(cache) {
			return cache.size();
		}
	}

	public void put(K key, V value) {
		synchronized(cache) {
			cache.put(key, value);
		}
	}

	public V get(K key) {
		synchronized(cache) {
			return cache.get(key);
		}
	}
}