package test;

import monkey.MonkeyHashMap;
import util.MapFactory;
import util.MapImplementation;

import java.io.PrintWriter;
import java.util.Map;

import static test.ConcurrencySettings.MAP_IMPLEMENTATION;
import static test.ConcurrencySettings.MAP_LOAD_FACTOR;
import static test.ConcurrencySettings.MAP_SIZE;
import static test.ConcurrencySettings.NUM_THREADS;
import static test.ConcurrencySettings.N_EXPERIMENTS;
import static test.ConcurrencySettings.SINGLE_THREAD_ALSO_READS;
import static test.ConcurrencySettings.WARM_UP_REPETITIONS;
import static util.MapImplementation.MONKEY_HASH_MAP;

public class HashMapConcurrencyTester {

    public static void main(String[] args) throws InterruptedException {

        System.out.println(ConcurrencySettings.spillGuts());
        System.out.println("\n-----------------------------------------------\n");

        if (MAP_IMPLEMENTATION == null) {
            for (MapImplementation mapImplementation : MapImplementation.values()) {
                warmUpAndRunTest(mapImplementation);
            }
        } else {
            // warm up
            warmUpAndRunTest(MAP_IMPLEMENTATION);
        }
    }

    private static void warmUpAndRunTest(MapImplementation mapImplementation) throws InterruptedException {
        // warm up
        runTestsForMapImplementation(mapImplementation, true);

        // test
        runTestsForMapImplementation(mapImplementation, false);
    }

    private static void runTestsForMapImplementation(MapImplementation mapImplementation,
                                                     boolean warmUp) throws InterruptedException {
        Thread[] threads;

        long totalTime = 0;
        long maxTime = 0;
        String output = "x = 2:16\ny = [";

        if (warmUp) {
            System.out.printf("\n\nWARMING UP FOR TESTS WITH %s...\n", mapImplementation);
        }

        int nExperiments = warmUp ? WARM_UP_REPETITIONS : N_EXPERIMENTS;

        for (int numThreads = 2; numThreads <= 16; numThreads++) {
            totalTime = 0;
            threads = new Thread[numThreads];
            for (int experiment = 1; experiment <= nExperiments; experiment++) {

                if (!warmUp) {
                    System.out.printf("\n\nRUNNING EXPERIMENT %d WITH %s...\n\n",
                            experiment, mapImplementation);
                }

                Map<Long, Long> map = MapFactory.getNewMap(
                        mapImplementation,
                        ConcurrencySettings.PRE_ALLOCATE_MAP_CAPACITY ? 1 +
                                (int) Math.ceil(MAP_SIZE / MAP_LOAD_FACTOR) : 0,
                        Long::longValue,
                        MAP_LOAD_FACTOR);

                for (int i = 0; i < numThreads; i++) {
                    threads[i] = new Thread(new HashMapAccessRunnable(
                            i + 1, map, MAP_SIZE, i == 0, i > 0 || numThreads == 1 && SINGLE_THREAD_ALSO_READS, !warmUp));
                }

                long start = System.currentTimeMillis();

                for (int i = 0; i < numThreads; i++) {
                    threads[i].start();
                }

                for (int i = 0; i < numThreads; i++) {
                    threads[i].join();
                }

                long elapsed = System.currentTimeMillis() - start;
                totalTime += elapsed;
                if (elapsed > maxTime) {
                    maxTime = elapsed;
                }

                if (mapImplementation == MONKEY_HASH_MAP && !warmUp) {
                    System.out.println("Max hash functions = " + ((MonkeyHashMap) map).getHashesInUse());
                }
            }

            if (!warmUp) {
                System.out.println(String.format("\nAverage time = %.3f seconds.",
                        totalTime / (nExperiments * 1000f)));
                output += totalTime / (nExperiments * 1000f) + " "; //output += totalTime*2000f / (nExperiments * 5f*n) + " ";
                System.out.println(String.format("Max time = %.3f seconds.",
                        maxTime / 1000f));
                System.out.println("\n\n-----------------------------\n\n");
            } else break;
        }

        if (!warmUp) {
            output += "]";
            try {
                PrintWriter outputFile = new PrintWriter("out_" + mapImplementation + MAP_SIZE, "UTF-8");
                outputFile.print(output);
                outputFile.close();
            } catch(Exception e) {
                System.out.println("File error!");
            }
        }
    }
}
