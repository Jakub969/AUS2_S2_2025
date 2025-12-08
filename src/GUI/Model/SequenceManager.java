package GUI.Model;

import java.io.*;
import java.nio.file.*;

public class SequenceManager {
    private static final String SEQUENCE_FILE = "pcr_test_sequence.txt";
    private int currentValue;

    public SequenceManager() {
        this.currentValue = this.loadSequence();
    }

    private int loadSequence() {
        try {
            if (Files.exists(Paths.get(SEQUENCE_FILE))) {
                String content = Files.readString(Paths.get(SEQUENCE_FILE)).trim();
                return Integer.parseInt(content);
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println("Error loading sequence: " + e.getMessage());
        }
        return 0;
    }

    public synchronized int getNextValue() {
        this.currentValue++;
        return this.currentValue;
    }

    public synchronized int getCurrentValue() {
        return this.currentValue;
    }

    public void saveSequence() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(SEQUENCE_FILE))) {
            writer.write(String.valueOf(this.currentValue));
        } catch (IOException e) {
            System.err.println("Error saving sequence: " + e.getMessage());
        }
    }

    public synchronized void resetSequence(int value) {
        this.currentValue = value;
        this.saveSequence();
    }
}