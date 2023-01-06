package util;

import java.util.HashMap;
import java.util.Map;

public enum MapImplementation {

    SKIP_LIST("SKIP_LIST", true),
    MONKEY_HASH_MAP("MONKEY", true),
    CONCURRENT_HASH_MAP("CONCURRENT", true),
    HASH_MAP("HASH_MAP", false);

    private static Map<String, MapImplementation> mapTypeByShortName = new HashMap<>();

    static {
        for (MapImplementation enumeration : MapImplementation.values()) {
            mapTypeByShortName.put(enumeration.shortName, enumeration);
        }
    }

    private String shortName;
    private boolean threadSafe;

    MapImplementation(String shortName, boolean threadSafe) {
        this.shortName = shortName;
        this.threadSafe = threadSafe;
    }

    public static MapImplementation getByShortName(String shortName) {
        MapImplementation result = null;
        if (shortName != null) {
            result = mapTypeByShortName.get(shortName.toUpperCase());
        }
        return result != null ? result : HASH_MAP;
    }

    public boolean isThreadSafe() {
        return threadSafe;
    }

    public String getShortName() {
        return shortName;
    }
}
