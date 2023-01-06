package monkey;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class is a hash-based implementation of Map. It is intended to be thread-safe in scenarios where
 * there is but a single writer thread, which should never be interrupted or put in wait status.
 * It allows for concurrent reads and even supports iteration over the entry set, without using synchronization
 * or locks of any kind. It is eventually consistent, in the sense that:
 *  (i) an entry that has just been inserted may not be seen immediately thereafter by all reading threads, but
 *      after some (negligible) lag; after it is first seen, though, never (again) should it fail to be successfully
 *      retrieved by each reading thread; and
 * (ii) a mapped value that is changed may also not be immediately seen by all reading threads. In applications
 *      where the mapped value is an immutable reference to a (mutable) object, that is even less of a problem
 *      (and the object contents may or may not be volatile, at the user's discretion).
 *
 * It is required that one knows beforehand the maximum number of entries that may ever exist at a same time
 * in the map (its maximum capacity), for memory is pre-allocated to guarantee that the intended load factor is
 * respected without the need of rehashes.
 *
 * It provides O(1) worst-case time complexity for reads, writes and deletes, which is unusual in hash map
 * implementations, but it presents a (negligible) probability that an insertion may fail. Using the default
 * load factor of 0.5, which means the size of the pre-allocated underlying array will be twice the map's maximum
 * capacity, and the default cap of 50 for the number of hash functions to be used, the probability that an
 * insertion fails when the number of keys in the map is maxCapacity - 1 is less than 2^(-50), which is
 * ridiculously small for most practical applications.
 *
 * Here is how it works. We have a universal family of hash functions h_i(.), for i in {1, ..., maxHashes},
 * where maxHashes is typically a small constant. When a key x is inserted, we look for the first position in
 * the underlying array that is not occupied, following the sequence h_1(x), h_2(x), ..., up to h_maxHashes(x).
 * We insert x there (or simply overwrite its value if it already exists). If the sequence of hash lookups was
 * exhausted and we still have not found a spot for x, the insertion fails. When we look up for a key x, we
 * follow that same sequence of hash-calculated indexes in the array until we have exceeded the maximum number
 * of hash functions that was actually called for by any insertion operation. Such actual maximum is kept track of
 * dynamically. Having found the key, we return the corresponding value. The constructor accepts as parameter
 * a function that returns an object of type K (the key type) from an informed object of type V (the value type).
 * E.g, say we have a map where the types are Long for keys and User for values, and the key in each entry is a
 * Long resulting from User.getId(). In this case, we can simply pass User::getId to the constructor.
 * Such a function, if not null, will be used in each get() call to make sure that the retrieved value does
 * correspond to the intended key. This allows us to improve performance and reduce GC burden by reusing
 * deleted entries without possibly incurring in concurrency issues. If such a function is not informed,
 * then entries are deleted physically (never reused), and get() calls are safe anyway, albeit with a higher
 * GC toll owing to deletes.
 *
 * We can iterate through the collections of entries, keys and values in thread-safe fashion by traversing
 * the underlying array while skipping null/empty positions. Insertions and deletions done while an iteration
 * is taking place might not be immediately seen (eventual consistency).
 *
 * @param <K> The type of the map keys
 * @param <V> The type of mapped values
 */
public class MonkeyHashMap<K,V> implements Map<K,V> {

    private static final float DEFAULT_LOAD_FACTOR = 0.5f;
    private static final int DEFAULT_MAX_HASHES = 50;

    private Random random = new Random();

    private MonkeyHashMapEntry<K,V>[] entries;
    private int maxCapacity;
    private int size;
    private int arrayLength;
    private int bitMaskForSmartMod;

    private int maxHashes;
    private int hashesInUse;
    private int[] entryCountByNumberOfHashesUsed;

    private Set<Entry<K,V>> entrySet;
    private Set<K> keySet;
    private Collection<V> valueCollection;
    private final Function<V, K> valueToKeyFunction;

    public MonkeyHashMap(int maxCapacity) {
        this(maxCapacity, DEFAULT_LOAD_FACTOR);
    }

    public MonkeyHashMap(int maxCapacity, float loadFactor) {
        this(maxCapacity, loadFactor, null);
    }

    public MonkeyHashMap(int maxCapacity, Function<V, K> valueToKeyFunction) {
        this(maxCapacity, DEFAULT_LOAD_FACTOR, valueToKeyFunction);
    }

    public MonkeyHashMap(int maxCapacity, float loadFactor, Function<V, K> valueToKeyFunction) {
        this(maxCapacity, loadFactor, valueToKeyFunction, DEFAULT_MAX_HASHES);
    }

    @SuppressWarnings("unchecked")
    public MonkeyHashMap(int maxCapacity, float loadFactor, Function<V, K> valueToKeyFunction, int maxHashes) {
        this.maxCapacity = maxCapacity;
        this.arrayLength = getNextPowerOfTwo(1 + (int) Math.ceil(maxCapacity / loadFactor));
        this.bitMaskForSmartMod = this.arrayLength - 1;
        this.entries = (MonkeyHashMapEntry[]) new MonkeyHashMapEntry[this.arrayLength];
        this.maxHashes = maxHashes;
        this.size = 0;
        this.hashesInUse = 0;
        this.entryCountByNumberOfHashesUsed = new int[maxHashes + 1];
        this.valueToKeyFunction = valueToKeyFunction;
        createAbstractCollections();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return getEntry(key, false) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        for (MonkeyHashMapEntry<K,V> entry : entries) {
            if (entry != null && Objects.equals(entry.value, value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        MonkeyHashMapEntry<K,V> entry = getEntry(key, false);
        if (entry == null) {
            return null;
        }
        V value = entry.value;
        if (!validateMapping(key, value)) {
            return null;
        }
        return value;
    }

    private boolean validateMapping(Object key, V value) {
        if (value == null) {
            return false;
        }
        return this.valueToKeyFunction == null ||  // in this case we won't be reusing map entries anyway
                Objects.equals(key, this.valueToKeyFunction.apply(value));
    }

    @Override
    public V put(K key, V value) {
        MonkeyHashMapEntry<K,V> entry = getEntry(key, true);
        if (entry == null) {
            throw new IllegalArgumentException("Put operation failed for key " + key +
                    ". Exceeded number of attempts.");
        }
        K newEntryKey = entry.key;
        if (newEntryKey == null) {  // insert
            if (size == maxCapacity) {
                throw new IllegalArgumentException(("Put operation failed for key " + key +
                        ". The maximum capacity has been reached."));
            }
            ++size;
            entry.key = key;
        }
        V oldValue = entry.value;
        entry.value = value;
        return oldValue;
    }

    @Override
    public V remove(Object key) {
        MonkeyHashMapEntry<K,V> entry = getEntry(key, false);
        if (entry != null) {
            V value = entry.value;
            removeEntry(entry);
            return value;
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        for (int i = 0; i < arrayLength; i++) {
            entries[i] = null;
        }
        size = 0;
        for (int i = 1; i <= hashesInUse; i++) {
            entryCountByNumberOfHashesUsed[i] = 0;
        }
        hashesInUse = 0;
    }

    @Override
    public Set<Entry<K,V>> entrySet() {
        return this.entrySet;
    }

    @Override
    public Set<K> keySet() {
        return this.keySet;
    }

    @Override
    public Collection<V> values() {
        return this.valueCollection;
    }

    /**
     * Returns the maximum number of hash functions that were required by any single insertion.
     */
    public int getHashesInUse() {
        return hashesInUse;
    }

    /**
     * Just pops a random entry out of the map, returning its value.
     */
    V popRandomValue() {
        while (true) {
            MonkeyHashMapEntry<K,V> entry = entries[random.nextInt(arrayLength - 1)];
            if (entry != null && entry.key != null) {
                V value = entry.value;
                removeEntry(entry);
                return value;
            }
        }
    }

    /**
     * This method is used for both reads and writes.
     *
     * For reads, it tries to find the intended key calling up to this.hashesInUse, which is kept track of.
     *
     * For writes, it performs a read and, if the key exists, overwrites its value; otherwise, it uses the
     * first available spot that was found along the read attempts to insert the new key; if no available
     * spot was found thus far, it carries on searching for an available spot until a total of max_hashes
     * functions have been tried, in which case it would return null.
     *
     * @param key The desired key
     * @param upsertIntended true, if an insertion/update will take place; false, if a simple read is intended
     *
     * @return The (new) map entry associated to the informed key;
     *         or null, if either
     *            (1) upsertIntended is false and the key was not found, or
     *            (2) upsertIntended is true and we have exhausted maxHashes without finding a suitable spot
     */
    private MonkeyHashMapEntry<K,V> getEntry(Object key, boolean upsertIntended) {
        Integer firstAvailablePosition = null;
        int hashNumberUsedForFirstAvailablePosition = 0;

        int maxHashesForLookup = upsertIntended ? maxHashes : hashesInUse;

        for (int hashNumber = 1; hashNumber <= maxHashesForLookup; hashNumber++) {
            int hash = (hashNumber == 1 ? Objects.hashCode(key) : Objects.hash(hashNumber, key)) & bitMaskForSmartMod;
            MonkeyHashMapEntry<K,V> entry = entries[hash];
            if (entry != null && Objects.equals(entry.key, key)) {
                return entry;  // updates and successful reads should return from here
            }
            if (upsertIntended) {
                if (firstAvailablePosition == null && (entry == null || entry.key == null)) {
                    firstAvailablePosition = hash;
                    hashNumberUsedForFirstAvailablePosition = hashNumber;
                }
                if (firstAvailablePosition != null && hashNumber > hashesInUse) {
                    break;  // there's no hope of finding the key anyway, and we already found a spot for insertion
                }
            }
        }
        if (!upsertIntended) {
            return null;  // the key was not found
        }
        if (firstAvailablePosition == null) {
            return null;  // we have exhausted all hash functions and have not found an empty spot
        }
        MonkeyHashMapEntry<K,V> availableEntry = entries[firstAvailablePosition];
        if (availableEntry == null) {  // lazy instantiation
            availableEntry = new MonkeyHashMapEntry<>(null, null, firstAvailablePosition);
            entries[firstAvailablePosition] = availableEntry;
        }
        availableEntry.numberOfHashesUsed = hashNumberUsedForFirstAvailablePosition;
        addContributionToNumberOfHashesUsed(availableEntry);
        return availableEntry;
    }

    /**
     * Removes an entry logically, i.e., the Entry object is cleared for reuse, but not freed.
     */
    private void removeEntry(MonkeyHashMapEntry<K,V> entry) {
        removeContributionToNumberOfHashesUsed(entry);
        if (valueToKeyFunction != null) {
            // we're reusing map entries for performance sake
            entry.key = null;
            entry.numberOfHashesUsed = 0;
            entry.value = null;
        } else {
            this.entries[entry.positionInArray] = null;
        }
        size--;
    }

    private void addContributionToNumberOfHashesUsed(MonkeyHashMapEntry<K,V> entry) {
        int numberOfHashesUsed = entry.numberOfHashesUsed;
        entryCountByNumberOfHashesUsed[numberOfHashesUsed]++;
        if (numberOfHashesUsed > hashesInUse) {
            hashesInUse = numberOfHashesUsed;
        }
    }

    private void removeContributionToNumberOfHashesUsed(MonkeyHashMapEntry<K,V> entry) {
        int numberOfHashesUsed = entry.numberOfHashesUsed;
        entryCountByNumberOfHashesUsed[numberOfHashesUsed]--;
        if (entryCountByNumberOfHashesUsed[numberOfHashesUsed] == 0 && hashesInUse == numberOfHashesUsed) {
            for (int i = numberOfHashesUsed - 1; i >= 1; i--) {
                if (entryCountByNumberOfHashesUsed[i] > 0) {
                    hashesInUse = i;
                    return;
                }
            }
            hashesInUse = 0;
        }
    }

    private void createAbstractCollections() {

        this.keySet = new AbstractSet<K>() {
            @Override
            public Iterator<K> iterator() {
                return new MonkeyHashMapKeyIterator();
            }

            @Override
            public int size() {
                return MonkeyHashMap.this.size;
            }
        };
    }

    private static class MonkeyHashMapEntry<K,V> implements Map.Entry<K,V> {
        K key;
        V value;
        int numberOfHashesUsed;
        final Integer positionInArray;

        MonkeyHashMapEntry(K key, V value, Integer positionInArray) {
            this.key = key;
            this.value = value;
            this.positionInArray = positionInArray;
        }

		 @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        @Override
        public K getKey() {
            return key;
        }
    }

    private class MonkeyHashMapKeyIterator implements Iterator<K> {

        private int i;
		private int arrayLength;
		private MonkeyHashMapEntry<K,V>[] entries;

        private MonkeyHashMapKeyIterator() {
            this.i = 0;
			this.arrayLength = MonkeyHashMap.this.arrayLength;
			this.entries = MonkeyHashMap.this.entries;
        }

        @Override
        public boolean hasNext() {
            return i < arrayLength;
        }

        @Override
        public K next() {
            for(; i < arrayLength; i++)
				if(entries[i] != null)
					return entries[i++].key;
			return null;
        }
    }

    /**
     * Returns the smallest positive power of 2 that is greater than or equal to the given value.
     */
    private static int getNextPowerOfTwo(int value) {
        if (value <= 0) return 1;
        value--;
        value |= value >>> 1;
        value |= value >>> 2;
        value |= value >>> 4;
        value |= value >>> 8;
        value |= value >>> 16;
        value++;
        return value;
    }
}
