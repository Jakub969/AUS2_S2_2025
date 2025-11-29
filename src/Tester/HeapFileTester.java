package Tester;

import DS.Block;
import DS.HeapFile;
import Interface.IRecord;

import java.util.*;

public class HeapFileTester<T extends IRecord<T>> {

    private static class IndexedRecord<R> {
        final int blockIndex;
        final R record;

        IndexedRecord(int blockIndex, R record) {
            this.blockIndex = blockIndex;
            this.record = record;
        }
    }

    private final HeapFile<T> heapFile;
    private final List<List<IRecord<T>>> expectedBlocks;
    private final Random random;
    private final List<IndexedRecord<T>> inserted;

    public HeapFileTester(HeapFile<T> heapFile, long seed) {
        this.heapFile = heapFile;
        this.expectedBlocks = new ArrayList<>();
        this.random = new Random(seed);
        this.inserted = new ArrayList<>();
        this.loadExistingHeapState();
    }

    private void loadExistingHeapState() {
        int totalBlocks = this.heapFile.getTotalBlocks();
        for (int i = 0; i < totalBlocks; i++) {
            Block<T> block = this.heapFile.getBlock(i);
            List<IRecord<T>> list = new ArrayList<>();

            for (int j = 0; j < block.getValidCount(); j++) {
                IRecord<T> rec = block.getRecordAt(j);
                list.add(rec);
                this.inserted.add(new IndexedRecord<>(i, (T) rec));
            }
            this.expectedBlocks.add(list);
        }
    }

    public void insertRecord(T record) {
        int blockIndex = this.heapFile.insertRecord(record);

        while (this.expectedBlocks.size() <= blockIndex) {
            this.expectedBlocks.add(new ArrayList<>());
        }

        this.expectedBlocks.get(blockIndex).add(record);
        this.inserted.add(new IndexedRecord<>(blockIndex, record));
        this.validateState(blockIndex);
    }

    private void validateState(int blockIndex) {
        Block<T> block = this.heapFile.getBlock(blockIndex);
        List<IRecord<T>> expectedRecords = this.expectedBlocks.get(blockIndex);

        if (block.getValidCount() != expectedRecords.size()) {
            throw new IllegalStateException("Validation failed: valid count mismatch in block " + blockIndex);
        }

        for (IRecord<T> expectedRecord : expectedRecords) {
            boolean found = false;
            for (int j = 0; j < block.getValidCount(); j++) {
                if (block.getRecordAt(j).isEqual((T) expectedRecord)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalStateException("Validation failed: record not found in block " + blockIndex);
            }
        }
    }

    public void removeRecord(IndexedRecord<T> entry) {
        boolean removedHeap = this.heapFile.deleteRecord(entry.blockIndex, entry.record);

        boolean removedExpected = this.expectedBlocks.get(entry.blockIndex).removeIf(r -> r.isEqual(entry.record));

        if (removedHeap != removedExpected) {
            throw new IllegalStateException("Delete mismatch: heap vs expected");
        }

        this.trimExpectedBlocks();
    }

    public void findRecord(IndexedRecord<T> entry) {
        T fromHeap = this.heapFile.findRecord(entry.blockIndex, entry.record);

        boolean expectedFound = this.expectedBlocks.get(entry.blockIndex).stream().anyMatch(r -> r.isEqual(entry.record));

        boolean heapFound = (fromHeap != null);

        if (heapFound != expectedFound) {
            throw new IllegalStateException("Find mismatch: heap vs expected");
        }
    }

    public T generateRandomRecord() {
        if (!this.heapFile.getRecordClass().equals(Osoba.class)) {
            throw new IllegalStateException("This tester only supports Osoba");
        }
        return (T) Osoba.generateRandom();
    }

    public void performRandomOperations(int count) {
        for (int i = 0; i < count; i++) {

            int op = this.random.nextInt(3);

            switch (op) {
                case 0 -> {
                    T rec = this.generateRandomRecord();
                    this.insertRecord(rec);
                    System.out.println("[INSERT] " + rec);
                }
                case 1 -> {
                    if (!this.inserted.isEmpty()) {
                        IndexedRecord<T> entry = this.inserted.remove(this.random.nextInt(this.inserted.size()));
                        this.removeRecord(entry);
                        System.out.println("[DELETE] " + entry.record);
                    }
                }
                case 2 -> {
                    if (!this.inserted.isEmpty()) {
                        IndexedRecord<T> entry = this.inserted.get(this.random.nextInt(this.inserted.size()));
                        this.findRecord(entry);
                        System.out.println("[FIND] " + entry.record);
                    }
                }
            }
            this.printHeap();
            this.printExpected();
        }
    }

    private void trimExpectedBlocks() {
        while (!this.expectedBlocks.isEmpty() && this.expectedBlocks.get(this.expectedBlocks.size() - 1).isEmpty()) {
            this.expectedBlocks.remove(this.expectedBlocks.size() - 1);
        }
    }

    public void printHeap() {
        for (int i = 0; i < this.expectedBlocks.size(); i++) {
            System.out.println("Block " + i + ":");
            Block<T> block = this.heapFile.getBlock(i);
            System.out.println("Valid records: " + block.getValidCount());
            block.printRecords();
        }
    }

    public void printExpected() {
        for (int i = 0; i < this.expectedBlocks.size(); i++) {
            System.out.println("Expected Block " + i + ": " + this.expectedBlocks.get(i));
        }
    }
}
