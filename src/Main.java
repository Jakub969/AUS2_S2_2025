import DS.HeapFile;
import GUI.Controller.AppController;
import GUI.View.MainWindow;
import Tester.Osoba;
import Tester.StructureTester;

public class Main {
    public static void main(String[] args) {
        for (int test = 0; test < 20; test++) {
            long seed = System.currentTimeMillis();
            System.out.println("Testujem zo seedom: " + seed);

            HeapFile<Osoba> heap = new HeapFile<>("osobyHeap.bin", Osoba.class, 1024);
            StructureTester<Osoba> tester = new StructureTester<>(heap, seed);

            try {
                tester.performRandomOperations(500);
                System.out.println("Seed: " + seed + " OK");
            } catch (Exception e) {
                System.out.println("Chyba pri seede: " + seed);
                e.printStackTrace();
                return;
            }
        }
        HeapFile<Osoba> heap = new HeapFile<>("osobyHeap.bin", Osoba.class, 1024);
        AppController controller = new AppController(heap);
        new MainWindow(controller);
    }
}
