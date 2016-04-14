package com.alvin.common.tools;

/**
 * @author lialiu
 */
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ConcurrentLRUMap<K, V> {
	private final ConcurrentMap<K, MapNode<K, V>> map;
	private final int capacity;
	private final ListWalker walker = new ListWalker();

	public ConcurrentLRUMap(int capacity, Class<? extends ConcurrentMap> cls) throws InstantiationException, IllegalAccessException {
		this.capacity = capacity;
		this.map = cls.newInstance();
		walker.setDaemon(true);
		walker.start();
	}
	
	public void printList(){
		walker.printList();
	}

	public void put(K key, V value) {
		MapNode<K, V> node = new MapNode<K, V>(key, value);
		walker.touch(node);
		map.put(key, node);
	}

	public V get(K key) {
		MapNode<K, V> node = map.get(key);
		if (node != null) {
			walker.touch(node);
			return node.data;
		}
		return null;
	}

	public boolean contains(K key) {
		MapNode<K, V> node = map.get(key);
		if (node != null) {
			walker.touch(node);
			return true;
		}
		return false;
	}

	public void remove(K key) {
		MapNode<K, V> node = map.remove(key);
		if (node != null) {
			walker.remove(node);
		}
	}
	
	public Set<K> keySet(){
		return map.keySet();
	}

	public int size() {
		return map.size();
	}

	public void stop() {
		walker.stopRunning();
	}

	public void finalize() {
		walker.stopRunning();
	}

	public void printMap() {
		for (K key : map.keySet()) {
			System.out.print(map.get(key).data + ",");
		}
		System.out.println();
		walker.printList();
	}

	static private class MapNode<K, V>

	{

		private V data;
		private K key;
		private MapNode<K, V> prev;
		private MapNode<K, V> next;

		public MapNode() {
		}

		private MapNode(K key, V data) {
			this.data = data;
			this.key = key;
		}
	}

	private class ListWalker extends Thread {
		private final BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
		private MapNode<K, V> dummyNode = new MapNode<K, V>();;
		private MapNode<K, V> head = dummyNode;
		private boolean runningFlag = true;
		private int listLength = 0;

		public ListWalker() {
		}

		public void printList() {
			MapNode<K, V> node = head;
			while (node != dummyNode) {
				System.out.print(node.data + "->");
				node = node.next;
			}
			System.out.println();
		}

		public void touch(MapNode<K, V> node) {
			queue.offer(new Event(Event.TOUCH, node));
		}

		public void remove(MapNode<K, V> node) {
			queue.offer(new Event(Event.REMOVE, node));
		}

		public void stopRunning() {
			runningFlag = false;
		}

		@Override
		public void run() {
			while (runningFlag) {
				try {
					Event event = queue.take();
					if (event.action == Event.TOUCH) {
						MapNode<K, V> node = event.data;
						unLink(node);
						toHead(node);
						listLength++;
						evict();
//						if (map.size() > capacity) {
//							//use queued evict to avoid map full too fast and latest touch not effected 
//							queue.offer(new Event(Event.EVICT, null));
//						}
					} else if (event.action == Event.REMOVE) {
						unLink(event.data);
						listLength--;
					}
//					else {
//						evict();
//					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		private void evict() {
			if (listLength > capacity) {
				System.out.println("evict, size=" + listLength+"," +map.size());
				MapNode<K, V> deadNode = dummyNode.prev;
				while (deadNode != null && listLength > capacity) {
					MapNode<K, V> prevNode = deadNode.prev;
					unLink(deadNode);
					map.remove(deadNode.key);
					listLength--;
					deadNode = prevNode;
				}
			}
		}

		private MapNode<K, V> unLink(MapNode<K, V> node) {
			if (node.prev != null) {
				node.prev.next = node.next;
			}
			if (node.next != null) {
				node.next.prev = node.prev;
			}
			node.next = null;
			node.prev = null;
			return node;
		}

		private MapNode<K, V> toHead(MapNode<K, V> node) {
			if (node == head) {
				return node;
			}
			MapNode<K, V> oldHead = head;
			head = node;
			head.next = oldHead;
			oldHead.prev = head;
			return node;
		}

		private class Event

		{
			public static final String TOUCH = "touch";
			public static final String REMOVE = "remove";
//			public static final String EVICT = "evict";

			private MapNode<K, V> data;
			private String action;

			public Event(String action, MapNode<K, V> data) {
				this.action = action;
				this.data = data;
			}
		}
	}
}