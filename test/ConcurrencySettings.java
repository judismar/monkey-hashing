package test;

import util.MapImplementation;

class ConcurrencySettings {

    // map implementation
    static final MapImplementation MAP_IMPLEMENTATION = null;  // null to loop through all

    // scenario
    static final int MAP_SIZE = 100_000;
    static final float MAP_LOAD_FACTOR = 0.5f;
    static final int NUM_THREADS = 4;
    static final boolean MULTIPLE_WRITERS = false;

    // complicators
    static final boolean INCLUDE_ITERATION_WHILE_READING = true;
    static final boolean CHANGE_EXISTING_ENTRIES = true;
    static final boolean INCLUDE_DELETIONS = false;
    static final boolean SINGLE_THREAD_ALSO_READS = true;

    // mitigators / fixes?
    static final boolean PRE_ALLOCATE_MAP_CAPACITY = true;

    // test config
    static final int WARM_UP_REPETITIONS = 1;
    static final int N_EXPERIMENTS = 4;
    static final int WRITER_THREAD_REPETITIONS = 1000;
    static final int READER_THREAD_REPETITIONS = 2000;
    static final long KEY_GAP_FOR_INSERTS = 583475513;
    static final long KEY_GAP_FACTOR_FOR_READS_AND_DELETIONS = 2;

    // logs
    static final boolean LOGS_ENABLED = true;

    static String spillGuts() {
        StringBuffer sb = new StringBuffer();
        sb.append("MAP_SIZE = ").append(MAP_SIZE)
                .append("\nMAP_LOAD_FACTOR = ").append(MAP_LOAD_FACTOR)
                .append("\nNUM_THREADS = ").append(NUM_THREADS)
                .append("\nMULTIPLE_WRITERS = ").append(MULTIPLE_WRITERS)
                .append("\n")
                .append("\nINCLUDE_ITERATION_WHILE_READING = ").append(INCLUDE_ITERATION_WHILE_READING)
                .append("\nCHANGE_EXISTING_ENTRIES = ").append(CHANGE_EXISTING_ENTRIES)
                .append("\nINCLUDE_DELETIONS = ").append(INCLUDE_DELETIONS)
                .append("\nSINGLE_THREAD_ALSO_READS = ").append(SINGLE_THREAD_ALSO_READS)
                .append("\n")
                .append("\nPRE_ALLOCATE_MAP_CAPACITY = ").append(PRE_ALLOCATE_MAP_CAPACITY)
                .append("\n")
                .append("\nWARM_UP_REPETITIONS = ").append(WARM_UP_REPETITIONS)
                .append("\nN_EXPERIMENTS = ").append(N_EXPERIMENTS)
                .append("\nWRITER_THREAD_REPETITIONS = ").append(WRITER_THREAD_REPETITIONS)
                .append("\nREADER_THREAD_REPETITIONS = ").append(READER_THREAD_REPETITIONS)
                .append("\nKEY_GAP_FOR_INSERTS = ").append(KEY_GAP_FOR_INSERTS)
                .append("\nKEY_GAP_FACTOR_FOR_READS_AND_DELETIONS = ").append(KEY_GAP_FACTOR_FOR_READS_AND_DELETIONS);
        return sb.toString();
    }
}

