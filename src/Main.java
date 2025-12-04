import DS.LinearHashFile;
import Tester.HashFileTester;
import Data.Osoba;

public class Main {
    public static void main(String[] args) {
        /*for (int test = 0; test < 20; test++) {
            long seed = System.currentTimeMillis();
            System.out.println("Testujem zo seedom: " + seed);

            HeapFile<Osoba> heap = new HeapFile<>("osobyHeap.bin", Osoba.class, 1024);
            HeapFileTester<Osoba> tester = new HeapFileTester<>(heap, seed);

            try {
                tester.performRandomOperations(500);
                System.out.println("Seed: " + seed + " OK");
            } catch (Exception e) {
                System.out.println("Chyba pri seede: " + seed);
                e.printStackTrace();
                return;
            }
        }*/

        long seed = System.currentTimeMillis();
        System.out.println("Testing with seed: " + seed);
        LinearHashFile<Osoba> hashFile = new LinearHashFile<>(
                Osoba.class,
                2,
                Osoba::getHash,
                "primary_data.bin",
                "overflow_data.bin",
                256,
                128
        );

        HashFileTester<Osoba> tester = new HashFileTester<>(
                hashFile,
                Osoba::getHash,
                seed
        );

        tester.performRandomOperations(1500);
        tester.printBucketDistribution();
    }
}
