package Tester;

import DS.LinearHashFile;
import Interface.IRecord;
import Interface.IHashable;

import java.util.*;
import java.util.function.Function;

public class HashFileTester<T extends IRecord<T> & IHashable> {

    private static class IndexedRecord<R> {
        final long key;
        final R record;

        IndexedRecord(long key, R record) {
            this.key = key;
            this.record = record;
        }
    }

    private final LinearHashFile<T> hashFile;
    private final Function<T, Long> keyExtractor;
    private final Random random;
    private final List<IndexedRecord<T>> inserted;
    private final Map<Long, T> expectedRecords; // Expected state: key -> record
    private final long seed;

    public HashFileTester(LinearHashFile<T> hashFile, Function<T, Long> keyExtractor, long seed) {
        this.hashFile = hashFile;
        this.keyExtractor = keyExtractor;
        this.seed = seed;
        this.random = new Random(seed);
        this.inserted = new ArrayList<>();
        this.expectedRecords = new HashMap<>();
    }

    public void insertRecord(T record) {
        long key = this.keyExtractor.apply(record);

        // Perform insertion
        this.hashFile.insert(record);

        long absKey = Math.abs(key);
        // Update expected state
        this.expectedRecords.put(absKey, record.createCopy());
        this.inserted.add(new IndexedRecord<>(absKey, record.createCopy()));

        System.out.println("[INSERT] Key: " + absKey + ", Record: " + record);
        this.validateState();
    }

    public void findRandomRecord() {
        if (this.inserted.isEmpty()) {
            System.out.println("[FIND] No records to find");
            return;
        }

        IndexedRecord<T> entry = this.inserted.get(this.random.nextInt(this.inserted.size()));
        T fromHashFile = this.hashFile.find(entry.record);

        T expected = this.expectedRecords.get(entry.key);
        boolean expectedFound = (expected != null);
        boolean hashFileFound = (fromHashFile != null);

        if (hashFileFound != expectedFound) {
            throw new IllegalStateException("Find mismatch: hashFile returned " + hashFileFound +
                    ", expected " + expectedFound + " for key: " + entry.key);
        }

        if (hashFileFound && !fromHashFile.isEqual(expected)) {
            throw new IllegalStateException("Record content mismatch for key: " + entry.key);
        }

        System.out.println("[FIND] Key: " + entry.key + ", Found: " + hashFileFound);
    }

    public void performRandomOperations(int count) {
        for (int i = 0; i < count; i++) {
            System.out.println("\n--- Operation " + (i + 1) + " ---");

            int op = 0;//this.random.nextInt(2); // 0=insert, 1=find

            switch (op) {
                case 0 -> {
                    T rec = this.generateRandomRecord();
                    this.insertRecord(rec);
                }
                case 1 -> this.findRandomRecord();
            }

            this.printStatistics();

            this.printBucketDistribution();
        }

        System.out.println("\n=== FINAL VALIDATION ===");
        this.validateState();
        this.printFinalStatistics();
    }

    public T generateRandomRecord() {
        if (!this.hashFile.getPrimaryFile().getRecordClass().equals(Osoba.class)) {
            throw new IllegalStateException("This tester only supports Osoba");
        }
        return (T) Osoba.generateRandom();
    }

    private void validateState() {
        System.out.println("Validating state...");

        // Check that all expected records are found in hash file
        for (Map.Entry<Long, T> entry : this.expectedRecords.entrySet()) {
            T expectedRecord = entry.getValue();
            T foundRecord = this.hashFile.find(expectedRecord);
            T again = this.hashFile.find(expectedRecord);

            if (foundRecord == null) {
                throw new IllegalStateException("Validation failed: Record with key " + entry.getKey() + " not found in hash file");
            }

            if (!foundRecord.isEqual(expectedRecord)) {
                throw new IllegalStateException("Validation failed: Record with key " + entry.getKey() + " content mismatch");
            }
        }

        // Check that inserted list matches expected records
        if (this.inserted.size() != this.expectedRecords.size()) {
            throw new IllegalStateException("Validation failed: Inserted list size " + this.inserted.size() +
                    " doesn't match expected records size " + this.expectedRecords.size());
        }

        System.out.println("State validation passed!");
    }

    public void printStatistics() {
        System.out.println("--- Statistics ---");
        System.out.println("Total records (expected): " + this.expectedRecords.size());
        System.out.println("Total buckets: " + this.hashFile.getNumberOfBuckets());
        System.out.println("Primary file blocks: " + this.hashFile.getPrimaryFile().getTotalBlocks());
        System.out.println("Overflow file blocks: " + this.hashFile.getOverflowFile().getTotalBlocks());
        System.out.println("Primary file records: " + this.hashFile.getPrimaryFile().getTotalRecords());
        System.out.println("Overflow file records: " + this.hashFile.getOverflowFile().getTotalRecords());
    }

    public void printFinalStatistics() {
        System.out.println("\n=== FINAL STATISTICS ===");
        this.printStatistics();
        System.out.println("All validations passed successfully!");
    }

    public void printBucketDistribution() {
        System.out.println("\n--- Bucket Distribution ---");
        int totalBuckets = this.hashFile.getNumberOfBuckets();

        for (int i = 0; i < totalBuckets; i++) {
            int recordsInBucket = this.countRecordsInBucket(i);
            int bucketPointer = this.hashFile.getBucketPointer(i);
            System.out.println("Bucket " + i + " (Bucket pointer: " + bucketPointer + "): " + recordsInBucket + " records");
        }
    }

    private int countRecordsInBucket(int bucket) {
        // This is a helper method that would need to be implemented based on your LinearHashFile structure
        // You might need to add a method to LinearHashFile to count records per bucket
        int count = 0;
        try {
            // Try to traverse the chain and count records
            // This depends on your LinearHashFile implementation details
            // You might need to add a method like getBucketRecordCount(int bucket) to LinearHashFile
            count = this.hashFile.getBucketRecordCount(bucket);
        } catch (Exception e) {
            // If we can't count, return -1
            return -1;
        }
        return count;
    }
}