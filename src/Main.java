import DS.Block;
import DS.ChainedBlock;
import DS.HeapFile;
import DS.LinearHashFile;
import GUI.View.MainWindow;
import Tester.HashFileTester;
import Data.Osoba;
import Tester.HeapFileTester;

import javax.swing.*;

public class Main {
    public static void main(String[] args) {
        /*Class<Block<Osoba>> blockClass = (Class<Block<Osoba>>) (Class<?>) Block.class;
        HeapFileTester<Osoba> heapFileTester = new HeapFileTester<>(new HeapFile<>("test_heap_file.bin", Osoba.class, blockClass,512), System.currentTimeMillis());
        heapFileTester.performRandomOperations(1000);*/
        /*long seed = System.currentTimeMillis();
        System.out.println("Testing with seed: " + seed);
        LinearHashFile<Osoba> hashFile = new LinearHashFile<>(
                Osoba.class,
                4,
                Osoba::getHash,
                "primary_data.bin",
                "overflow_data.bin",
                1024,
                512
        );

        HashFileTester<Osoba> tester = new HashFileTester<>(hashFile,Osoba::getHash,seed);tester.performRandomOperations(1500);tester.printBucketDistribution();*/
        //MainWindow<Osoba> mainWindow = new MainWindow<>(hashFile);mainWindow.setVisible(true);
    }
}
