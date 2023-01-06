package test;

import java.util.Map;

import static test.ConcurrencySettings.*;

class HashMapAccessRunnable implements Runnable {

    private final Map<Long, Long> hashMap;

    private final int threadNumber;
    private final int maxEntries;
    private final boolean doWrite;
    private final boolean doRead;
    private final boolean verbose;

    private final static long tooBigAnInt = Integer.MAX_VALUE;

    private volatile static boolean doneFirstLoad = false;
    private volatile static boolean aborted = false;

    HashMapAccessRunnable(int threadNumber, Map<Long, Long> map, int maxEntries, boolean doWrite, boolean doRead,
                          boolean verbose) {
        aborted = false;
        this.threadNumber = threadNumber;
        this.hashMap = map;
        this.maxEntries = maxEntries;
        this.doWrite = doWrite;
        this.doRead = doRead;
        this.verbose = verbose;
    }

    public void run() {
        if (aborted) {
            return;
        }

        if (ConcurrencySettings.LOGS_ENABLED && verbose) {
            System.out.printf("Starting thread %d (write=%s, read=%s)...\n", threadNumber, doWrite, doRead);
        }

        try {
            if (doWrite) {  // MAIN WRITER THREAD
                doneFirstLoad = false;

                for (int rep = 1; rep <= WRITER_THREAD_REPETITIONS; rep++) {
                    if (INCLUDE_DELETIONS || !doneFirstLoad) {
                        for (long i = 0; i < KEY_GAP_FOR_INSERTS * maxEntries; i += KEY_GAP_FOR_INSERTS) {
                            hashMap.put(i, i);
                        }
                        doneFirstLoad = true;
                    }

                    if (INCLUDE_DELETIONS) {
                        for (long i = 0; i < KEY_GAP_FOR_INSERTS * maxEntries;
                             i += KEY_GAP_FOR_INSERTS * KEY_GAP_FACTOR_FOR_READS_AND_DELETIONS) {
                            hashMap.remove(i);
                        }
                    }
                    if (CHANGE_EXISTING_ENTRIES) {
                        for (long i = 0; i < KEY_GAP_FOR_INSERTS * maxEntries; i += KEY_GAP_FOR_INSERTS) {
                            hashMap.put(i, i);
                        }
                    }
                }
            }
            if (doRead) {
                for (int rep = 1; rep <= READER_THREAD_REPETITIONS; rep++) {
                    if (INCLUDE_ITERATION_WHILE_READING) {
                        for (Long key : hashMap.keySet()) {
                            if (key != null) {
                                accessMap(key, true);
                            }
                        }
                    } else {
                        for (long i = KEY_GAP_FOR_INSERTS; i < KEY_GAP_FOR_INSERTS * maxEntries;
                             i += KEY_GAP_FOR_INSERTS * KEY_GAP_FACTOR_FOR_READS_AND_DELETIONS) {
                            accessMap(i, false);
                        }
                    }
                }
            }

            if (ConcurrencySettings.LOGS_ENABLED && verbose) {
                System.out.printf("Thread %d%s has completed.\n",
                        threadNumber, doWrite ? " (WRITER)" : "");
            }

        } catch (Exception e) {
            System.out.printf("Thread %d%s was aborted with exception %s.\n",
                    threadNumber, doWrite ? " (WRITER)" : "", e);
            aborted = true;
        }

    }

    private void accessMap(Long key, boolean isIterating) {
        if (ConcurrencySettings.MULTIPLE_WRITERS) {
            writeAndGetFromMap(key);
        } else {
            readFromMap(key, isIterating);
        }
    }

    private void readFromMap(long key, boolean isIterating) {
        if (isIterating) {
            return;
        }
        Long retrieved = hashMap.get(key);
        if (doneFirstLoad && retrieved == null && !isEntryProneToDeletion(key)) {
            try {
                System.out.printf("Thread %d just read a null, will sleep briefly so it syncs up.\n", threadNumber);
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // retry
            retrieved = hashMap.get(key);
            if (retrieved == null) {  // is it still null?
                System.out.printf("Oh my God, it happened! A null was found for key %d.", key);
                System.exit(2);
            } else {
                System.out.printf("Thread %d is ok again.\n", threadNumber);
            }
        }
        if (retrieved != null) {
            if (retrieved != (tooBigAnInt + key) && retrieved != (tooBigAnInt + 2 * key) &&
                    (retrieved != key)) {
                System.out.printf("Oh my God, it happened! A weird value was found for key %d.\n", key);
                System.exit(3);
            }
        }
    }

    private boolean isEntryProneToDeletion(long key) {
        return key % (ConcurrencySettings.KEY_GAP_FACTOR_FOR_READS_AND_DELETIONS * KEY_GAP_FOR_INSERTS) == 0;
    }

    private void writeAndGetFromMap(long key) {
        hashMap.put(key, 3L * key);
        Long retrieved = hashMap.get(key);
        if (retrieved == null) {
            System.out.println("Oh my God, it happened! A null was found where there should have been something.");
            System.exit(4);
        }
    }
}