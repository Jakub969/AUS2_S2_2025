package GUI.Model;

import DS.LinearHashFile;
import Data.Osoba;
import Data.PCRTest;
import java.io.File;

public class Model {
    private LinearHashFile<Osoba> hashFileOsoba;
    private LinearHashFile<PCRTest> hashFilePCRTest;
    private final SequenceManager pcrTestSequence;
    private String currentOsobaFolder;
    private String currentPCRFolder;

    public Model() {
        this("osoba_data", "pcr_data");
    }

    // Constructor with folder names
    public Model(String osobaFolder, String pcrFolder) {
        this.currentOsobaFolder = osobaFolder;
        this.currentPCRFolder = pcrFolder;

        // Create folders if they don't exist
        this.createFolder(osobaFolder);
        this.createFolder(pcrFolder);

        this.hashFileOsoba = new LinearHashFile<>(Osoba.class, 4, Osoba::getHash,
                osobaFolder, 512, 256);
        this.hashFilePCRTest = new LinearHashFile<>(PCRTest.class, 4, PCRTest::getHash,
                pcrFolder, 512, 256);
        this.pcrTestSequence = new SequenceManager();
    }

    // Constructor with full folder paths
    public Model(String osobaFolderPath, String pcrFolderPath,
                 int initialBuckets, int blockSizePrimary, int blockSizeOverflow) {
        this.currentOsobaFolder = osobaFolderPath;
        this.currentPCRFolder = pcrFolderPath;

        // Create folders if they don't exist
        this.createFolder(osobaFolderPath);
        this.createFolder(pcrFolderPath);

        this.hashFileOsoba = new LinearHashFile<>(Osoba.class, initialBuckets,
                Osoba::getHash, osobaFolderPath, blockSizePrimary, blockSizeOverflow);
        this.hashFilePCRTest = new LinearHashFile<>(PCRTest.class, initialBuckets,
                PCRTest::getHash, pcrFolderPath, blockSizePrimary, blockSizeOverflow);
        this.pcrTestSequence = new SequenceManager();
    }

    public void vlozPCRTest(PCRTest test) {
        this.hashFilePCRTest.insert(test);
        Osoba dummy = Osoba.fromUUID(test.getUUIDPacienta());
        Osoba osoba = this.hashFileOsoba.find(dummy);
        osoba.pridatTest(test.getKodTestu());
        this.hashFileOsoba.edit(osoba);
    }

    public Osoba vyhladatOsobu(Osoba osoba) {
        return this.hashFileOsoba.find(osoba);
    }

    public PCRTest vyhladatPCR(PCRTest test) {
        return this.hashFilePCRTest.find(test);
    }

    public void vlozOsobu(Osoba osoba) {
        this.hashFileOsoba.insert(osoba);
    }

    public void editOsoba(Osoba osoba) {
        this.hashFileOsoba.edit(osoba);
    }

    public void editPCR(PCRTest test) {
        this.hashFilePCRTest.edit(test);
    }

    public void generujUdaje(int pocet) {
        for (int i = 0; i < pocet; i++) {
            Osoba osoba = Osoba.generateRandom();
            this.vlozOsobu(osoba);
            int testCount = (int) (Math.random() * 5) + 1;
            for (int j = 0; j < testCount; j++) {
                PCRTest test = new PCRTest(
                        new java.util.Date(System.currentTimeMillis() - (long)(Math.random() * 1_000_000_000)),
                        osoba.getUUID(),
                        this.pcrTestSequence.getNextValue(),
                        !(Math.random() < 0.5),
                        Math.random() * 100.0,
                        "Poznamka" + (int)(Math.random() * 100)
                );
                this.vlozPCRTest(test);
            }
        }
    }

    private void createFolder(String folderPath) {
        File folder = new File(folderPath);
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    // Close all files
    public void close() {
        if (this.hashFileOsoba != null) {
            this.hashFileOsoba.close();
        }
        if (this.hashFilePCRTest != null) {
            this.hashFilePCRTest.close();
        }
    }

    // Create new Osoba file in a different folder
    public void createNewOsobaFile(String folderPath, int initialBuckets,
                                   int blockSizePrimary, int blockSizeOverflow) {
        // Close existing file if open
        if (this.hashFileOsoba != null) {
            this.hashFileOsoba.close();
        }

        // Create folder
        this.createFolder(folderPath);

        // Create new file
        this.hashFileOsoba = new LinearHashFile<>(Osoba.class, initialBuckets,
                Osoba::getHash, folderPath, blockSizePrimary, blockSizeOverflow);
        this.currentOsobaFolder = folderPath;
    }

    // Create new PCR file in a different folder
    public void createNewPCRFile(String folderPath, int initialBuckets,
                                 int blockSizePrimary, int blockSizeOverflow) {
        // Close existing file if open
        if (this.hashFilePCRTest != null) {
            this.hashFilePCRTest.close();
        }

        // Create folder
        this.createFolder(folderPath);

        // Create new file
        this.hashFilePCRTest = new LinearHashFile<>(PCRTest.class, initialBuckets,
                PCRTest::getHash, folderPath, blockSizePrimary, blockSizeOverflow);
        this.currentPCRFolder = folderPath;
    }

    // Open existing Osoba file
    public void openOsobaFile(String folderPath) {
        // Close existing file if open
        if (this.hashFileOsoba != null) {
            this.hashFileOsoba.close();
        }

        // Check if folder exists and has valid files
        File dirFile = new File(folderPath + File.separator + "directory.txt");
        File primaryFile = new File(folderPath + File.separator + "primary_data.bin");

        if (!dirFile.exists() || !primaryFile.exists()) {
            throw new IllegalArgumentException("Invalid Osoba folder: " + folderPath);
        }

        // Use default settings for existing files
        this.hashFileOsoba = new LinearHashFile<>(Osoba.class, 4,
                Osoba::getHash, folderPath, 512, 256);
        this.currentOsobaFolder = folderPath;
    }

    // Open existing PCR file
    public void openPCRFile(String folderPath) {
        // Close existing file if open
        if (this.hashFilePCRTest != null) {
            this.hashFilePCRTest.close();
        }

        // Check if folder exists and has valid files
        File dirFile = new File(folderPath + File.separator + "directory.txt");
        File primaryFile = new File(folderPath + File.separator + "primary_data.bin");

        if (!dirFile.exists() || !primaryFile.exists()) {
            throw new IllegalArgumentException("Invalid PCR folder: " + folderPath);
        }

        // Use default settings for existing files
        this.hashFilePCRTest = new LinearHashFile<>(PCRTest.class, 4,
                PCRTest::getHash, folderPath, 512, 256);
        this.currentPCRFolder = folderPath;
    }

    // Getters for current folder paths
    public String getCurrentOsobaFolder() {
        return this.currentOsobaFolder;
    }

    public String getCurrentPCRFolder() {
        return this.currentPCRFolder;
    }

    // Check if files are loaded
    public boolean isOsobaFileLoaded() {
        return this.hashFileOsoba != null;
    }

    public boolean isPCRFileLoaded() {
        return this.hashFilePCRTest != null;
    }

    public LinearHashFile<Osoba> getHashFileOsoba() {
        return this.hashFileOsoba;
    }

    public LinearHashFile<PCRTest> getHashFilePCRTest() {
        return this.hashFilePCRTest;
    }
}
