package com.alvin.common.tools;

/**
 * 
 * @author lialiu
 *
 */
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;

public class BloomFilter {
	private static final float DEFAULT_FALSE_POISTIVE_RATE = 0.01f;
	private final AtomicInteger elementCount = new AtomicInteger(0);
	private final int FILTER_SIZE_FACTOR;
	private final int HASH_COUNT;
	private final float FALSE_POISTIVE_RATE;
	private static final int HASH_FACTOR = 30;
	private int capacity;
	private int mask;
	private BitSet filter = null;

	/**
	 * 
	 * @param capacity
	 *            Design capacity of Bloom Filter instance
	 */
	public BloomFilter(int capacity) {
		this(capacity, DEFAULT_FALSE_POISTIVE_RATE);
	}

	/**
	 * 
	 * @param capacity
	 *            Design capacity of Bloom Filter instance
	 * @param falsePositiveRate
	 *            Expect False Positive rate
	 */
	public BloomFilter(int capacity, float falsePositiveRate) {
		// FILTER_SIZE_FACTOR = Integer.MAX_VALUE
		// & (int) Math.ceil(1 / (Math.log(0.6185) /
		// Math.log(falsePositiveRate)));
		this.capacity = capacity;
		mask = capacity - 1;
		FALSE_POISTIVE_RATE = falsePositiveRate;
		FILTER_SIZE_FACTOR = (int) Math.round(-2.08134700350803 * Math.log(FALSE_POISTIVE_RATE));
		filter = new BitSet(FILTER_SIZE_FACTOR * capacity);
		HASH_COUNT = (int) Math.round(-Math.log(FALSE_POISTIVE_RATE) / Math.log(2));
	}

	public void getStatus() {
		System.out.println("===Bloom Filter Status===");
		System.out.println("FILTER_SIZE_FACTOR=" + FILTER_SIZE_FACTOR);
		System.out.println("HASH_COUNT=" + HASH_COUNT);
		System.out.println("elementCount=" + elementCount.get());
		System.out.println("capacity=" + capacity);
		System.out.println("mask=" + mask);
		System.out.println("filterSize=" + filter.size());
		System.out.println("currentFalsePositiveRate=" + currentFalsePositiveRate());
		System.out.println("===END===");
	}

	public boolean add(String data) {
		int[] bits = hashAll(data);
		int i = 0;
		for (; i < bits.length; i++) {
			if (!filter.get(bits[i])) {
				break;
			}
		}
		elementCount.incrementAndGet();
		if (i < bits.length) {
			for (; i < bits.length; i++) {
				filter.set(bits[i]);
			}
			return true;
		} else {
			return false;
		}
	}

	public boolean contain(String data) {
		int[] bits = hashAll(data);
		for (int i = 0; i < bits.length; i++) {
			if (!filter.get(bits[i])) {
				return false;
			}
		}
		return true;
	}

	public double currentFalsePositiveRate() {
		return Math.pow(1 - Math.pow(Math.E, -HASH_COUNT * 1.0D * elementCount.get() / filter.size()), HASH_COUNT);
	}

	public void clear() {
		filter = new BitSet(FILTER_SIZE_FACTOR * capacity);
		elementCount.set(0);
	}

	private int[] hashAll(String data) {
		int[] result = new int[HASH_COUNT];
		for (int i = 0; i < HASH_COUNT; i++) {
			result[i] = hashOne(data, i);
		}
		return result;
	}

	private int hashOne(String data, int salt) {
		int hash = 0;
		for (int i = 0; i < data.length(); i++) {
			hash = (HASH_FACTOR + salt) * hash + data.charAt(i);
		}
		return hash & mask;
	}

	public static void main(String[] args) {
		BloomFilter bf = new BloomFilter(100, 0.01f);
		bf.getStatus();
		System.out.println(bf.add("teststr"));
		System.out.println(bf.add("teststr"));
		System.out.println(bf.contain("teststr"));
		for (int i = 1; i < 100; i++) {
			bf.add(Math.random() + "a");
		}
		bf.getStatus();
		for (int i = 1; i < 100; i++) {
			bf.add(Math.random() + "a");
		}
		bf.getStatus();
		bf.clear();
		bf.getStatus();
		System.out.println(bf.contain("teststr"));
	}
}
