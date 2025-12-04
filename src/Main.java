import DS.LinearHashFile;
import GUI.View.MainWindow;
import Tester.HashFileTester;
import Data.Osoba;

import javax.swing.*;

public class Main {
    public static void main(String[] args) {

        long seed = System.currentTimeMillis();
        System.out.println("Testing with seed: " + seed);
        LinearHashFile<Osoba> hashFile = new LinearHashFile<>(
                Osoba.class,
                2,
                Osoba::getHash,
                "primary_data.bin",
                "overflow_data.bin",
                128,
                128
        );

        //HashFileTester<Osoba> tester = new HashFileTester<>(hashFile,Osoba::getHash,seed);tester.performRandomOperations(1500);tester.printBucketDistribution();
        MainWindow<Osoba> mainWindow = new MainWindow<>(hashFile);mainWindow.setVisible(true);
    }
}
