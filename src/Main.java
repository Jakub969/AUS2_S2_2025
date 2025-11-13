import DS.HeapFile;
import Tester.Osoba;
import Tester.StructureTester;

public class Main {
    public static void main(String[] args) {
        long seed = System.currentTimeMillis();
        System.out.println("Using seed: " + seed);
        HeapFile<Osoba> heap = new HeapFile<>("osobyHeap.bin", Osoba.class, 4096);
        StructureTester<Osoba> tester = new StructureTester<>(heap, 1763042688094L);
        tester.performRandomOperations(50);

        tester.printHeap();
        tester.printExpected();
    }
}
