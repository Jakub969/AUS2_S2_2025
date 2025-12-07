package GUI.Controller;

import DS.ChainedBlock;
import GUI.Model.Model;
import Data.Osoba;
import Data.PCRTest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Controller {
    private Model model;

    public Controller() {
        this.model = null;
    }

    public void createNewModelWithParams(String osobaFolderPath, String pcrFolderPath, int initialBuckets, int blockSizePrimary, int blockSizeOverflow) {
        if (this.model != null) {
            this.model.close();
        }
        this.model = new Model(osobaFolderPath, pcrFolderPath, initialBuckets, blockSizePrimary, blockSizeOverflow);
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

    public boolean insertOsoba(String meno, String priezvisko, Date datumNarodenia, String uuid) {
        if (this.model == null) return false;

        try {
            Osoba osoba = new Osoba(meno, priezvisko, datumNarodenia, uuid, new Integer[6]);
            this.model.vlozOsobu(osoba);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public List<PCRTest> getPCRTestsForPerson(String uuid) {
        List<PCRTest> tests = new ArrayList<>();

        if (this.model == null) {
            return tests;
        }

        try {
            Osoba search = Osoba.fromUUID(uuid);
            Osoba foundPerson = this.model.vyhladatOsobu(search);

            if (foundPerson == null) {
                return tests;
            }

            Integer[] testCodes = foundPerson.getTestyPacienta();

            for (Integer testCode : testCodes) {
                if (testCode != null) {
                    PCRTest testSearch = PCRTest.fromTestID(testCode);
                    PCRTest foundTest = this.model.vyhladatPCR(testSearch);
                    if (foundTest != null) {
                        tests.add(foundTest);
                    }
                }
            }

            return tests;
        } catch (Exception e) {
            return tests;
        }
    }

    public String getPersonWithTestsFormatted(String uuid) {
        StringBuilder result = new StringBuilder();

        if (this.model == null) {
            return "Model not loaded.";
        }

        try {
            Osoba search = Osoba.fromUUID(uuid);
            Osoba foundPerson = this.model.vyhladatOsobu(search);

            if (foundPerson == null) {
                return "Person not found.";
            }
            result.append("=== PERSON DETAILS ===\n");
            result.append(foundPerson).append("\n\n");

            List<PCRTest> tests = this.getPCRTestsForPerson(uuid);

            result.append("=== PCR TESTS (").append(tests.size()).append(") ===\n");
            if (tests.isEmpty()) {
                result.append("No PCR tests found for this person.\n");
            } else {
                for (int i = 0; i < tests.size(); i++) {
                    result.append("\nTest ").append(i + 1).append(":\n");
                    result.append(tests.get(i).toString()).append("\n");
                }
            }

            return result.toString();
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    public boolean editOsoba(String meno, String priezvisko, Date datumNarodenia, String uuid) {
        if (this.model == null) return false;

        try {
            Osoba osoba = new Osoba(meno, priezvisko, datumNarodenia, uuid, new Integer[6]);
            this.model.editOsoba(osoba);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

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

    public boolean editPCRTest(Date datumTestu, double hodnota, boolean vysledok, String poznamka, int kodTestu) {
        if (this.model == null) return false;

        try {
            PCRTest test = new PCRTest(datumTestu, "", kodTestu, vysledok, hodnota, poznamka);
            this.model.editPCR(test);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void generateData(int count) {
        if (this.model != null) {
            this.model.generujUdaje(count);
        }
    }

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

    public String getOsobaBucketsInfo() {
        if (this.model == null || this.model.getHashFileOsoba() == null) {
            return "Osoba file not loaded.";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("=== OSOBA BUCKETS ===\n");

        int totalBuckets = this.model.getHashFileOsoba().getPrimaryFile().getTotalBlocks();
        sb.append("Total Buckets: ").append(totalBuckets).append("\n");
        sb.append("Total records: ").append(this.model.getHashFileOsoba().getPrimaryFile().getTotalRecords() +
                this.model.getHashFileOsoba().getOverflowFile().getTotalRecords()).append("\n\n");

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
        sb.append("=== OSOBA BUCKETS ===\n");

        int totalBuckets = this.model.getHashFilePCRTest().getPrimaryFile().getTotalBlocks();
        sb.append("Total Buckets: ").append(totalBuckets).append("\n");
        sb.append("Total records: ").append(this.model.getHashFilePCRTest().getPrimaryFile().getTotalRecords() +
                this.model.getHashFilePCRTest().getOverflowFile().getTotalRecords()).append("\n\n");

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
}