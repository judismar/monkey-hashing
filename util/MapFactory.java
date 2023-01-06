package util;

import monkey.MonkeyHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

public class MapFactory {

    public static <K,V> Map<K,V> getNewMap(MapImplementation mapType, int capacity, Float loadFactor) {
        return getNewMap(mapType, capacity, null, loadFactor);
    }

    public static <K,V> Map<K,V> getNewMap(MapImplementation mapType, int capacity,
                                           Function<V,K> valueToKeyFunction) {
        return getNewMap(mapType, capacity, valueToKeyFunction, null);
    }

    public static <K,V> Map<K,V> getNewMap(MapImplementation mapType, int capacity,
                                           Function<V,K> valueToKeyFunction, Float loadFactor) {
        switch (mapType) {
            case SKIP_LIST:
                return new ConcurrentSkipListMap<>();
            case CONCURRENT_HASH_MAP:
                return loadFactor == null ?
                        new ConcurrentHashMap<>(capacity) :
                        new ConcurrentHashMap<>(capacity, loadFactor);
            case MONKEY_HASH_MAP:
                return loadFactor == null ?
                        new MonkeyHashMap<>(capacity, valueToKeyFunction) :
                        new MonkeyHashMap<>(capacity, loadFactor, valueToKeyFunction);
            case HASH_MAP: default:
                return loadFactor == null ?
                        new HashMap<>(capacity) :
                        new HashMap<>(capacity, loadFactor);
        }
    }
}
