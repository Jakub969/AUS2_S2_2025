package GUI.Controller;

import DS.ChainedBlock;
import GUI.Model.Model;
import Data.Osoba;
import Data.PCRTest;
import java.util.Date;

public class Controller {
    private Model model;

    public Controller() {
        this.model = null;
    }

    // File operations
    public void createNewModel(String osobaFolderPath, String pcrFolderPath) {
        if (this.model != null) {
            this.model.close();
        }
        this.model = new Model(osobaFolderPath, pcrFolderPath);
    }

    public void createNewModelWithParams(String osobaFolderPath, String pcrFolderPath,
                                         int initialBuckets, int blockSizePrimary, int blockSizeOverflow) {
        if (this.model != null) {
            this.model.close();
        }
        this.model = new Model(osobaFolderPath, pcrFolderPath,
                initialBuckets, blockSizePrimary, blockSizeOverflow);
    }

    public void openExistingModel(String osobaFolderPath, String pcrFolderPath) {
        if (this.model != null) {
            this.model.close();
        }
        this.model = new Model(osobaFolderPath, pcrFolderPath);
    }

    public void closeModel() {
        if (this.model != null) {
            this.model.close();
            this.model = null;
        }
    }

    // Osoba operations
    public boolean insertOsoba(String meno, String priezvisko, Date datumNarodenia, String uuid) {
        if (this.model == null) return false;

        try {
            Osoba osoba = new Osoba(meno, priezvisko, datumNarodenia, uuid);
            this.model.vlozOsobu(osoba);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Osoba findOsoba(String uuid) {
        if (this.model == null) return null;

        try {
            Osoba search = Osoba.fromUUID(uuid);
            return this.model.vyhladatOsobu(search);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean editOsoba(Osoba osoba) {
        if (this.model == null) return false;

        try {
            this.model.editOsoba(osoba);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // PCR Test operations
    public boolean insertPCRTest(Date datumTestu, String uuidPacienta, int kodTestu,
                                 boolean vysledokTestu, double hodnotaTestu, String poznamka) {
        if (this.model == null) return false;

        try {
            PCRTest test = new PCRTest(datumTestu, uuidPacienta, kodTestu,
                    vysledokTestu, hodnotaTestu, poznamka);
            this.model.vlozPCRTest(test);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public PCRTest findPCRTest(int kodTestu) {
        if (this.model == null) return null;

        try {
            PCRTest search = PCRTest.fromTestID(kodTestu);
            return this.model.vyhladatPCR(search);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean editPCRTest(PCRTest test) {
        if (this.model == null) return false;

        try {
            this.model.editPCR(test);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Data generation
    public void generateData(int count) {
        if (this.model != null) {
            this.model.generujUdaje(count);
        }
    }

    // File management
    public void createNewOsobaFile(String folderPath, int initialBuckets,
                                   int blockSizePrimary, int blockSizeOverflow) {
        if (this.model != null) {
            this.model.createNewOsobaFile(folderPath, initialBuckets,
                    blockSizePrimary, blockSizeOverflow);
        }
    }

    public void createNewPCRFile(String folderPath, int initialBuckets,
                                 int blockSizePrimary, int blockSizeOverflow) {
        if (this.model != null) {
            this.model.createNewPCRFile(folderPath, initialBuckets,
                    blockSizePrimary, blockSizeOverflow);
        }
    }

    public void openOsobaFile(String folderPath) {
        if (this.model != null) {
            this.model.openOsobaFile(folderPath);
        }
    }

    public void openPCRFile(String folderPath) {
        if (this.model != null) {
            this.model.openPCRFile(folderPath);
        }
    }

    // Status checks
    public boolean isModelLoaded() {
        return this.model != null;
    }

    public boolean isOsobaFileLoaded() {
        return this.model != null && this.model.isOsobaFileLoaded();
    }

    public boolean isPCRFileLoaded() {
        return this.model != null && this.model.isPCRFileLoaded();
    }

    public String getCurrentOsobaFolder() {
        return this.model != null ? this.model.getCurrentOsobaFolder() : "No file loaded";
    }

    public String getCurrentPCRFolder() {
        return this.model != null ? this.model.getCurrentPCRFolder() : "No file loaded";
    }

    // Bucket display (returns formatted string instead of direct access)
    public String getOsobaBucketsInfo() {
        if (this.model == null || this.model.getHashFileOsoba() == null) {
            return "Osoba file not loaded.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("=== OSOBA BUCKETS ===\n\n");

        int totalBuckets = this.model.getHashFileOsoba().getPrimaryFile().getTotalBlocks();

        for (int i = 0; i < totalBuckets; i++) {
            sb.append("Bucket ").append(i).append(":\n");
            int cur = i;
            ChainedBlock<Osoba> b = this.model.getHashFileOsoba().getPrimaryFile().getBlock(cur);
            sb.append("Primary file Block ").append(cur).append(", NextBlockIndex: ").append(b.getNextBlockIndex()).append(" (").append(b.getValidCount()).append(" records):\n");
            for (int j = 0; j < b.getValidCount(); j++) {
                sb.append("    ").append(b.getRecordAt(j)).append("\n");
            }
            cur = b.getNextBlockIndex();
            while (cur != -1) {
                b = this.model.getHashFileOsoba().getOverflowFile().getBlock(cur);
                sb.append("   Overflow file Block ").append(cur).append(", NextBlockIndex: ").append(b.getNextBlockIndex()).append(" (").append(b.getValidCount()).append(" records):\n");
                for (int j = 0; j < b.getValidCount(); j++) {
                    sb.append("    ").append(b.getRecordAt(j)).append("\n");
                }
                cur = b.getNextBlockIndex();
            }
        }

        return sb.toString();
    }

    public String getPCRBucketsInfo() {
        if (this.model == null || this.model.getHashFilePCRTest() == null) {
            return "PCR file not loaded.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("=== OSOBA BUCKETS ===\n\n");

        int totalBuckets = this.model.getHashFilePCRTest().getPrimaryFile().getTotalBlocks();

        for (int i = 0; i < totalBuckets; i++) {
            sb.append("Bucket ").append(i).append(":\n");
            int cur = i;
            ChainedBlock<PCRTest> b = this.model.getHashFilePCRTest().getPrimaryFile().getBlock(cur);
            sb.append("Primary file Block ").append(cur).append(", NextBlockIndex: ").append(b.getNextBlockIndex()).append(" (").append(b.getValidCount()).append(" records):\n");
            for (int j = 0; j < b.getValidCount(); j++) {
                sb.append("    ").append(b.getRecordAt(j)).append("\n");
            }
            cur = b.getNextBlockIndex();
            while (cur != -1) {
                b = this.model.getHashFilePCRTest().getOverflowFile().getBlock(cur);
                sb.append("   Overflow file Block ").append(cur).append(", NextBlockIndex: ").append(b.getNextBlockIndex()).append(" (").append(b.getValidCount()).append(" records):\n");
                for (int j = 0; j < b.getValidCount(); j++) {
                    sb.append("    ").append(b.getRecordAt(j)).append("\n");
                }
                cur = b.getNextBlockIndex();
            }
        }
        return sb.toString();
    }

    public Model getModel() {
        return this.model;
    }
}