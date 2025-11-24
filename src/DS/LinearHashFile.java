package DS;

import Interface.IHashable;
import Interface.IRecord;

import java.io.*;
import java.util.*;
import java.util.function.Function;

public class LinearHashFile<T extends IRecord<T> & IHashable> {
    private final HeapFile<T> heapFile;
    private final Class<T> recordClass;
    private final int blockSize;
    private final Function<T, String> keyExtractor;

    private List<Integer> bucketPointers; // head block index per bucket, -1 if empty
    private int i;           // current level: buckets = 2^i initially
    private int nextSplit;
    private final File dirFile;

    public LinearHashFile(HeapFile<T> heapFile, Class<T> recordClass, int initialBuckets, Function<T,String> keyExtractor, String baseFileName) {
        if (initialBuckets <= 0 || (initialBuckets & (initialBuckets - 1)) != 0)
            throw new IllegalArgumentException("initialBuckets must be power of two and > 0");

        this.heapFile = heapFile;
        this.recordClass = recordClass;
        this.blockSize = heapFile.getBlockSize();
        this.keyExtractor = keyExtractor;
        this.dirFile = new File(baseFileName + "_dir.txt");

        this.i = Integer.numberOfTrailingZeros(initialBuckets);
        this.nextSplit = 0;

        if (this.dirFile.exists()) this.loadDirectory();
        else {
            this.bucketPointers = new ArrayList<>();
            for (int b = 0; b < initialBuckets; b++) this.bucketPointers.add(-1);
            this.saveDirectory();
        }
    }

    private int bucketForKey(String key) {
        int h = Math.abs(key.hashCode());
        int mod = (1 << this.i);
        int bucket = h & (mod - 1);
        if (bucket < this.nextSplit) {
            bucket = h & ((mod << 1) - 1);
        }
        return bucket;
    }

    private void saveDirectory() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(this.dirFile))) {
            pw.println(this.i);
            pw.println(this.nextSplit);
            for (int ptr : this.bucketPointers) pw.println(ptr);
        } catch (IOException e) {
            throw new RuntimeException("Error saving hash directory", e);
        }
    }

    private void loadDirectory() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.dirFile))) {
            this.i = Integer.parseInt(br.readLine().trim());
            this.nextSplit = Integer.parseInt(br.readLine().trim());
            this.bucketPointers = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                this.bucketPointers.add(Integer.parseInt(line.trim()));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error loading hash directory", e);
        }
    }

    // -------------------- API --------------------

    public void insert(T record) {
        String key = this.keyExtractor.apply(record);
        int bucket = this.bucketForKey(key);
        this.insertIntoBucket(bucket, record);
    }

    public T findByKey(String key) {
        int bucket = this.bucketForKey(key);
        int head = this.bucketPointers.get(bucket);
        if (head == -1) return null;
        int idx = head;
        while (idx != -1) {
            Block<T> block = this.heapFile.getBlock(idx);
            for (int r = 0; r < block.getBlockFactor(); r++) {
                IRecord<T> rec = block.getRecordAt(r);
                if (rec != null) {
                    T cand = (T) rec;
                    if (key.equals(this.keyExtractor.apply(cand))) {
                        return cand.createCopy();
                    }
                }
            }
            idx = block.getNextBlockIndex();
        }
        return null;
    }

    public boolean deleteByKey(String key) {
        int bucket = this.bucketForKey(key);
        int head = this.bucketPointers.get(bucket);
        if (head == -1) return false;

        int idx = head;
        while (idx != -1) {
            Block<T> block = this.heapFile.getBlock(idx);
            // find record
            IRecord<T> toRemove = null;
            for (int r = 0; r < block.getBlockFactor(); r++) {
                IRecord<T> rec = block.getRecordAt(r);
                if (rec != null) {
                    T cand = (T) rec;
                    if (key.equals(this.keyExtractor.apply(cand))) {
                        toRemove = rec;
                        break;
                    }
                }
            }
            if (toRemove != null) {
                block.removeRecord((T) toRemove); // compacts block
                // if block empty and not head -> unlink and optionally free
                if (block.getValidCount() == 0) {
                    int prev = block.getPreviousBlockIndex();
                    int next = block.getNextBlockIndex();
                    if (prev != -1) {
                        Block<T> prevB = this.heapFile.getBlock(prev);
                        prevB.setNextBlockIndex(next);
                        this.heapFile.writeBlockToFile(prevB, prev);
                    } else {
                        // block was head of chain
                        this.bucketPointers.set(bucket, next);
                    }
                    if (next != -1) {
                        Block<T> nextB = this.heapFile.getBlock(next);
                        nextB.setPreviousBlockIndex(prev);
                        this.heapFile.writeBlockToFile(nextB, next);
                    }
                    // optionally reclaim physical block - left for HeapFile
                } else {
                    this.heapFile.writeBlockToFile(block, idx);
                }
                this.saveDirectory();
                this.heapFile.trimTrailingEmptyBlocks();
                return true;
            }
            idx = block.getNextBlockIndex();
        }
        return false;
    }

    // -------------------- insertion helpers --------------------

    private void insertIntoBucket(int bucket, T record) {
        int head = this.bucketPointers.get(bucket);
        if (head == -1) {
            int newIdx = this.heapFile.allocateNewBlock();
            Block<T> block = this.heapFile.getBlock(newIdx);
            block.addRecord(record);
            this.heapFile.writeBlockToFile(block, newIdx);
            this.bucketPointers.set(bucket, newIdx);
            this.saveDirectory();
            return;
        }

        int idx = head;
        while (true) {
            Block<T> block = this.heapFile.getBlock(idx);
            if (block.getValidCount() < block.getBlockFactor()) {
                block.addRecord(record);
                this.heapFile.writeBlockToFile(block, idx);
                return;
            }
            int next = block.getNextBlockIndex();
            if (next == -1) {
                // allocate new overflow block
                int newIdx = this.heapFile.allocateNewBlock();
                Block<T> newB = this.heapFile.getBlock(newIdx);
                block.setNextBlockIndex(newIdx);
                newB.setPreviousBlockIndex(idx);
                this.heapFile.writeBlockToFile(block, idx);
                newB.addRecord(record);
                this.heapFile.writeBlockToFile(newB, newIdx);

                // simple split trigger: split one bucket after overflow allocated
                this.splitNextBucketIfNeeded();
                return;
            } else idx = next;
        }
    }

    // -------------------- split logic --------------------

    private void splitNextBucketIfNeeded() {
        // simple policy: split always when overflow allocated, but avoid nested splits by single call
        this.splitNextBucket();
    }

    public void splitNextBucket() {
        int bucketToSplit = this.nextSplit;
        int oldHead = this.bucketPointers.get(bucketToSplit);

        // collect all records from chain
        List<T> all = new ArrayList<>();
        if (oldHead != -1) {
            int idx = oldHead;
            while (idx != -1) {
                Block<T> b = this.heapFile.getBlock(idx);
                for (int r = 0; r < b.getBlockFactor(); r++) {
                    IRecord<T> rec = b.getRecordAt(r);
                    if (rec != null) all.add(rec.createCopy());
                }
                idx = b.getNextBlockIndex();
            }
        }

        // create new bucket
        int newBucketIndex = this.bucketPointers.size();
        this.bucketPointers.add(-1);

        // clear old chain: easiest option is to reuse the first block as the new head and set next=-1
        if (oldHead == -1) {
            int newHeadIdx = this.heapFile.allocateNewBlock();
            this.bucketPointers.set(bucketToSplit, newHeadIdx);
        } else {
            // reset first block
            Block<T> primary = new Block<>(this.recordClass, this.blockSize);
            this.heapFile.writeBlockToFile(primary, oldHead);
            this.bucketPointers.set(bucketToSplit, oldHead);
        }

        // redistribute records using i+1 bits
        for (T rec : all) {
            String k = this.keyExtractor.apply(rec);
            int h = Math.abs(k.hashCode());
            int target = h & ((1 << (this.i + 1)) - 1); // mod 2^(i+1)
            if (target == bucketToSplit) this.insertIntoBucketNoSplit(bucketToSplit, rec);
            else this.insertIntoBucketNoSplit(newBucketIndex, rec);
        }

        this.nextSplit++;
        if (this.nextSplit >= (1 << this.i)) {
            this.nextSplit = 0;
            this.i++;
        }
        this.saveDirectory();
    }

    private void insertIntoBucketNoSplit(int bucket, T record) {
        int head = this.bucketPointers.get(bucket);
        if (head == -1) {
            int newIdx = this.heapFile.allocateNewBlock();
            Block<T> b = this.heapFile.getBlock(newIdx);
            b.addRecord(record);
            this.heapFile.writeBlockToFile(b, newIdx);
            this.bucketPointers.set(bucket, newIdx);
            return;
        }
        int idx = head;
        while (true) {
            Block<T> b = this.heapFile.getBlock(idx);
            if (b.getValidCount() < b.getBlockFactor()) {
                b.addRecord(record);
                this.heapFile.writeBlockToFile(b, idx);
                return;
            }
            int next = b.getNextBlockIndex();
            if (next == -1) {
                int newIdx = this.heapFile.allocateNewBlock();
                Block<T> nb = this.heapFile.getBlock(newIdx);
                b.setNextBlockIndex(newIdx);
                nb.setPreviousBlockIndex(idx);
                this.heapFile.writeBlockToFile(b, idx);
                nb.addRecord(record);
                this.heapFile.writeBlockToFile(nb, newIdx);
                return;
            }
            idx = next;
        }
    }

    // -------------------- utilities --------------------

    public int getNumberOfBuckets() {
        return this.bucketPointers.size();
    }

    public List<Integer> getBucketPointers() {
        return Collections.unmodifiableList(this.bucketPointers);
    }
}
