package com.alvin.common.tools;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
	private final ListWalker walker;

	public ConcurrentLRUMap(int capacity, Class<? extends ConcurrentMap> cls)
			throws InstantiationException, IllegalAccessException {
		this.capacity = capacity;
		this.map = cls.newInstance();
		walker = new ListWalker();
		walker.setDaemon(true);
		walker.start();
	}

	public void printList() {
		walker.printList();
	}

	public void checkMap() {
		walker.checkMap();
	}

	public void put(K key, V value) {
		MapNode<K, V> node = new MapNode<K, V>(key, value);
		MapNode<K, V> oldNode = map.putIfAbsent(key, node);

		// both Sync is necessary for here to avoid touch enqueue before add.
		if (oldNode != null) {
			synchronized (oldNode) {
				oldNode.data = value;
				walker.touch(oldNode);
			}
		} else {
			synchronized (node) {
				walker.add(node);
			}
		}
	}

	public V get(K key) {
		MapNode<K, V> node = map.get(key);
		if (node != null) {
			synchronized (node) {
				if(node.isAlive){
					walker.touch(node);
					return node.data;
				}
			}
		}
		return null;
	}

	public boolean contains(K key) {
		MapNode<K, V> node = map.get(key);
		if (node != null) {
			synchronized (node) {
				walker.touch(node);
				return true;
			}
		}
		return false;
	}

	public void remove(K key) {
		MapNode<K, V> node = map.remove(key);
		if (node != null) {
			walker.remove(node);
		}
	}

	public Set<K> keySet() {
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

	static private class MapNode<K, V> {
		private volatile V data;
		private volatile K key;
		private volatile MapNode<K, V> prev;
		private volatile MapNode<K, V> next;
		private volatile boolean isAlive = false;

		public MapNode() {
		}

		private MapNode(K key, V data) {
			synchronized (key) {
				this.data = data;
				this.key = key;
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((data == null) ? 0 : data.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MapNode other = (MapNode) obj;
			if (data == null) {
				if (other.data != null)
					return false;
			} else if (!data.equals(other.data))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}
	}

	private class ListWalker extends Thread {
		private final BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
		private final MapNode<K, V> tail = new MapNode<K, V>();
		private final MapNode<K, V> head = new MapNode<K, V>();
		private volatile boolean runningFlag = true;
		private volatile int listLength = 0;

		public ListWalker() {
			head.next = tail;
			tail.prev = head;
		}

		public void checkMap() {
			System.out.println("List size:" + size());
			System.out.println("Map size:" + map.size());
			for (MapNode<K, V> node = head.next; node != tail; node = node.next) {
				if (!map.containsKey(node.key)) {
					System.out.println("Key not in map:" + node.key);
				} else {
					map.remove(node.key);
				}
			}

			for (K key : map.keySet()) {
				System.out.println("Key not in list:" + map.get(key));
			}
		}

		public void printList() {
			 MapNode<K, V> node = head.next;
			 while (node != tail) {
			 System.out.print(node.data + "->");
			 node = node.next;
			 }
			 System.out.println();
		}

		public void touch(MapNode<K, V> node) {
			synchronized (node) {
				queue.offer(new Event(Event.TOUCH, node));
			}
		}

		public void add(MapNode<K, V> node) {
			synchronized (node) {
				queue.offer(new Event(Event.ADD, node));
			}
		}

		public void remove(MapNode<K, V> node) {
			synchronized (node) {
				queue.offer(new Event(Event.REMOVE, node));
			}
		}

		public void stopRunning() {
			runningFlag = false;
		}

		public int size() {
			return listLength;
		}

		@Override
		public void run() {
			while (runningFlag) {
				try {
					Event event = queue.take();
					MapNode<K, V> node = event.data;
					synchronized (node) {
						if (event.action == Event.TOUCH) {
							if (node.isAlive == false) {
//								System.out.println("Got dead");
								continue;
							}
							unLink(node);
							toHead(node);
						} else if (event.action == Event.ADD) {
							toHead(node);
							node.isAlive = true;
							listLength++;
						} else if (event.action == Event.REMOVE) {
							unLink(event.data);
							event.data.isAlive = false;
							listLength--;
						}
					}
					evict();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		private void evict() {
			if (map.size() > capacity) {
				MapNode<K, V> deadNode = tail.prev;
				while (deadNode != head && map.size() > capacity) {
					// this line fail at 100 thread 100 loop
					MapNode<K, V> prevNode = null;
					synchronized (deadNode) {
						prevNode = deadNode.prev;
						if (map.remove(deadNode.key) != null) {
							unLink(deadNode);
							deadNode.isAlive = false;
							listLength--;
						}
					}
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
			if (node == head.next) {
				return node;
			}
			MapNode<K, V> oldHead = head.next;
			head.next = node;
			node.prev = head;
			node.next = oldHead;
			oldHead.prev = node;
			return node;
		}

		private class Event

		{
			public static final String ADD = "add";
			public static final String TOUCH = "touch";
			public static final String REMOVE = "remove";

			private final MapNode<K, V> data;
			private final String action;

			public Event(String action, MapNode<K, V> data) {
				this.action = action;
				this.data = data;
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + ((action == null) ? 0 : action.hashCode());
				result = prime * result + ((data == null) ? 0 : data.hashCode());
				return result;
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj)
					return true;
				if (obj == null)
					return false;
				if (getClass() != obj.getClass())
					return false;
				Event other = (Event) obj;
				if (action == null) {
					if (other.action != null)
						return false;
				} else if (!action.equals(other.action))
					return false;
				if (data == null) {
					if (other.data != null)
						return false;
				} else if (!data.equals(other.data))
					return false;
				return true;
			}
		}
	}

	static int xx = 0;

	public static void main(String args[]) throws InterruptedException, InstantiationException, IllegalAccessException {
		final ConcurrentLRUMap<Integer, Integer> map = new ConcurrentLRUMap<Integer, Integer>(10,
				ConcurrentHashMap.class);

		for (xx = 0; xx < 10000; xx++) {
			Thread tr = new Thread() {
				Random rand = new Random(System.currentTimeMillis());

				public void run() {
					for (int j = 1; j < 1000; j++) {
						int num = rand.nextInt(j);
						if(num < j/2){
							map.get(rand.nextInt(j-1));
							
						}else{
							map.put(num, num);
						}
					}
				}
			};
			tr.setDaemon(true);
			tr.start();
		}

		 Thread.currentThread().sleep(2000);
		
//		 map.put(6,6);
//		 map.put(7,7);
//		 map.put(8,8);
		
		
//		Thread.currentThread().sleep(3000);

		map.printList();
		map.printMap();
		map.checkMap();
	}
}