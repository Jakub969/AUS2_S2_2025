package GUI.Model;

import DS.LinearHashFile;
import Data.Osoba;
import Data.PCRTest;

public class Model {
    private LinearHashFile<Osoba> hashFileOsoba;
    private LinearHashFile<PCRTest> hashFilePCRTest;

    public Model() {
        this.hashFileOsoba = new LinearHashFile<>(Osoba.class, 4, Osoba::getHash, "osoba_primary_data.bin", "osoba_overflow_data.bin", 512, 256);
        this.hashFilePCRTest = new LinearHashFile<>(PCRTest.class, 4, PCRTest::getHash, "pcrtest_primary_data.bin", "pcrtest_overflow_data.bin", 512, 256);
    }

    public void vlozPCRTest(PCRTest test) {
        this.hashFilePCRTest.insert(test);
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
}
