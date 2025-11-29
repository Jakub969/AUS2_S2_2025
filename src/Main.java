import DS.LinearHashFile;
import GUI.Controller.AppController;
import GUI.View.MainWindow;
import Tester.HashFileTester;
import Tester.Osoba;

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
        /*LinearHashFile<Osoba> hashFile = new LinearHashFile<>(Osoba.class, 4, Osoba::getHash, "primary_data.bin", "overflow_data.bin", 512, 256);
        //AppController controller = new AppController(hashFile);
        MainWindow<Osoba> mainWindow = new MainWindow<>(hashFile);
        mainWindow.setVisible(true);*/
        LinearHashFile<Osoba> hashFile = new LinearHashFile<>(
                Osoba.class,
                4, // initial buckets
                Osoba::getHash, // key extractor
                "primary_data.bin", // primary file name
                "overflow_data.bin", // overflow file name
                512, // primary block size
                256  // overflow block size
        );

        // Create tester
        HashFileTester<Osoba> tester = new HashFileTester<>(
                hashFile,
                Osoba::getHash,
                12345L // seed
        );

        // Perform random operations
        tester.performRandomOperations(150);

        // Print final bucket distribution
        tester.printBucketDistribution();
    }
}
