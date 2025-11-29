package DS;

import Interface.IHashable;
import Interface.IRecord;

import java.io.*;
import java.util.*;
import java.util.function.Function;

public class LinearHashFile<T extends IRecord<T> & IHashable> {
    private HeapFile<T> primaryFile;
    private HeapFile<T> overflowFile;
    private final Function<T, Long> keyExtractor;

    private List<Integer> bucketPointers; // head block index per bucket, -1 if empty
    private int i;           // current level: buckets = 2^i initially
    private int nextSplit;
    private final File dirFile;

    public LinearHashFile(Class<T> recordClass, int initialBuckets, Function<T,Long> keyExtractor, String primaryFileName, String overflowFileName, int blockSizePrimary, int blockSizeOverflow) {
        if (initialBuckets <= 0 || (initialBuckets & (initialBuckets - 1)) != 0) {
            throw new IllegalArgumentException("initialBuckets must be power of two and > 0");
        }
        this.primaryFile = new HeapFile<>(primaryFileName, recordClass, blockSizePrimary);
        this.overflowFile = new HeapFile<>(overflowFileName, recordClass, blockSizeOverflow);
        this.keyExtractor = keyExtractor;
        this.dirFile = new File(primaryFileName + "_dir.txt");

        this.i = Integer.numberOfTrailingZeros(initialBuckets);
        this.nextSplit = 0;

        if (this.dirFile.exists()) {
            this.loadDirectory();
        } else {
            this.bucketPointers = new ArrayList<>();
            for (int b = 0; b < initialBuckets; b++) {
                this.bucketPointers.add(-1);
            }
            this.saveDirectory();
        }
    }

    private int bucketForKey(long key) {
        long h = key & 0x7FFFFFFFFFFFFFFFL;
        int mod = (1 << this.i);
        long bucket = h & (mod - 1);
        if (bucket < this.nextSplit) {
            bucket = h & ((mod << 1) - 1);
        }
        return (int) bucket;
    }

    private void saveDirectory() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(this.dirFile))) {
            pw.println(this.i);
            pw.println(this.nextSplit);
            for (int ptr : this.bucketPointers) {
                pw.println(ptr);
            }
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

    public void insert(T record) {
        long key = this.keyExtractor.apply(record);
        int bucket = this.bucketForKey(key);
        this.insertIntoBucket(bucket, record);
    }

    public T find(T record) {
        long key = this.keyExtractor.apply(record);
        int bucket = this.bucketForKey(key);
        int headBlockIndex = this.bucketPointers.get(bucket);

        if (headBlockIndex == -1) {
            return null;
        }

        // Search in primary file chain
        T found = this.primaryFile.findInChain(headBlockIndex, record);
        if (found != null) {
            return found;
        }

        // If not found in primary, search in overflow starting from primary chain
        int currentIndex = headBlockIndex;
        while (currentIndex != -1) {
            Block<T> block = this.primaryFile.getBlock(currentIndex);
            int nextIndex = block.getNextBlockIndex();
            if (nextIndex != -1 && nextIndex >= this.primaryFile.getTotalBlocks()) {
                // This points to overflow file
                return this.overflowFile.findInChain(nextIndex, record);
            }
            currentIndex = nextIndex;
        }

        return null;
    }

    public boolean delete(T record) {
        long key = this.keyExtractor.apply(record);
        int bucket = this.bucketForKey(key);
        int headBlockIndex = this.bucketPointers.get(bucket);

        if (headBlockIndex == -1) {
            return false;
        }

        boolean deleted = false;

        if (this.primaryFile.deleteFromChain(headBlockIndex, record)) {
            deleted = true;
        } else {
            int currentIndex = headBlockIndex;
            while (currentIndex != -1) {
                Block<T> block = this.primaryFile.getBlock(currentIndex);
                int nextIndex = block.getNextBlockIndex();

                if (nextIndex != -1 && nextIndex >= this.primaryFile.getTotalBlocks()) {
                    if (this.overflowFile.deleteFromChain(nextIndex, record)) {
                        deleted = true;
                        break;
                    }
                }

                currentIndex = nextIndex;
            }
        }

        if (!deleted) {
            return false;
        }

        this.handlePostDeletionCleanup(bucket, headBlockIndex);

        // Now it is safe to trim blocks
        //this.primaryFile.trimTrailingEmptyBlocks();
        this.overflowFile.trimTrailingEmptyBlocks();

        return true;
    }


    private void handlePostDeletionCleanup(int bucket, int headBlockIndex) {
        // Check if head block became empty and update bucket pointer if needed
        Block<T> headBlock = this.primaryFile.getBlock(headBlockIndex);
        if (headBlock.getValidCount() == 0) {
            int nextBlock = headBlock.getNextBlockIndex();
            this.bucketPointers.set(bucket, nextBlock);
            if (nextBlock != -1) {
                // Update the new head's previous pointer
                HeapFile<T> nextFile = this.getFileForBlockIndex(nextBlock);
                Block<T> newHead = nextFile.getBlock(nextBlock);
                newHead.setPreviousBlockIndex(-1);
                nextFile.writeBlockToFile(newHead, nextBlock);
            }
            this.saveDirectory();
        }
    }

    // Helper methods to determine which file contains a block
    private HeapFile<T> getFileForBlockIndex(int blockIndex) {
        if (blockIndex < this.primaryFile.getTotalBlocks()) {
            return this.primaryFile;
        } else {
            return this.overflowFile;
        }
    }

    private Block<T> getBlockFromAppropriateFile(int blockIndex) {
        return this.getFileForBlockIndex(blockIndex).getBlock(blockIndex);
    }

    private void writeBlockToAppropriateFile(Block<T> block, int blockIndex) {
        this.getFileForBlockIndex(blockIndex).writeBlockToFile(block, blockIndex);
    }

    private void insertIntoBucket(int bucket, T record) {
        int headBlockIndex = this.bucketPointers.get(bucket);

        if (headBlockIndex == -1) {
            // First record in bucket - insert into primary file
            HeapFile.BlockInsertResult result = this.primaryFile.insertRecordAsNewBlock(record, -1, -1);
            this.bucketPointers.set(bucket, result.blockIndex);
            this.saveDirectory();
            return;
        }
        HeapFile.BlockInsertResult result = this.primaryFile.insertRecordWithMetadata(record, headBlockIndex, -1, -1);

        if (result.blockIndex == -1)  {
            HeapFile.BlockInsertResult newResult = this.overflowFile.insertRecordWithMetadata(record,headBlockIndex, -1, -1);
            if (newResult.blockIndex == -1)  {
                // No space - add new overflow block
                this.addNewOverflowBlock(headBlockIndex, record);
            }
        }

        this.splitNextBucketIfNeeded();
    }

    private Integer findBlockWithSpace(int startBlockIndex) {
        int currentIndex = startBlockIndex;

        while (currentIndex != -1) {
            if (this.getFileForBlockIndex(currentIndex).hasFreeSpace(currentIndex)) {
                return currentIndex;
            }
            currentIndex = this.getFileForBlockIndex(currentIndex).getNextBlockIndex(currentIndex);
        }
        return null;
    }

    private void addNewOverflowBlock(int chainHeadIndex, T record) {
        // Find last block in chain
        int lastBlockIndex = this.findLastBlockInChain(chainHeadIndex);

        // Insert new block in overflow file
        HeapFile.BlockInsertResult result = this.overflowFile.insertRecordAsNewBlock(record, -1, lastBlockIndex);
        int newBlockIndex = result.blockIndex;

        // Update previous block's next pointer
        HeapFile<T> previousFile = this.getFileForBlockIndex(lastBlockIndex);
        Block<T> previousBlock = previousFile.getBlock(lastBlockIndex);
        previousBlock.setNextBlockIndex(newBlockIndex);
        previousFile.writeBlockToFile(previousBlock, lastBlockIndex);
    }

    private int findLastBlockInChain(int startBlockIndex) {
        int currentIndex = startBlockIndex;
        int lastIndex = startBlockIndex;

        while (currentIndex != -1) {
            lastIndex = currentIndex;
            currentIndex = this.getFileForBlockIndex(currentIndex).getNextBlockIndex(currentIndex);
        }
        return lastIndex;
    }

    private void splitNextBucketIfNeeded() {
        // Only split if load factor exceeds threshold
        double loadFactor = (double) this.primaryFile.getTotalRecords() / (this.getNumberOfBuckets() * this.getAverageBlockCapacity());
        if (loadFactor > 0.75) { // Adjust threshold as needed
            this.splitNextBucket();
        }
    }

    private double getAverageBlockCapacity() {
        // Return average records per block
        return (double) this.primaryFile.getBlock(0).getBlockFactor();
    }

    public void splitNextBucket() {
        int bucketToSplit = this.nextSplit;
        int oldHead = this.bucketPointers.get(bucketToSplit);

        // collect all records from chain
        List<T> all = new ArrayList<>();
        if (oldHead != -1) {
            int idx = oldHead;
            Block<T> b = this.primaryFile.getBlock(idx);
            for (int r = 0; r < b.getBlockFactor(); r++) {
                IRecord<T> rec = b.getRecordAt(r);
                if (rec != null) {
                    all.add(rec.createCopy());
                }
            }
            idx = b.getNextBlockIndex();
            while (idx != -1) {
                b = this.overflowFile.getBlock(idx);
                for (int r = 0; r < b.getBlockFactor(); r++) {
                    IRecord<T> rec = b.getRecordAt(r);
                    if (rec != null) {
                        all.add(rec.createCopy());
                    }
                }
                idx = b.getNextBlockIndex();
            }
        }

        // create new bucket
        int newBucketIndex = this.bucketPointers.size();
        this.bucketPointers.add(-1);

        // clear old chain: easiest option is to reuse the first block as the new head and set next=-1
        if (oldHead == -1) {
            int newHeadIdx = this.overflowFile.allocateEmptyBlock();
            this.bucketPointers.set(bucketToSplit, newHeadIdx);
        } else {
            // reset first block
            Block<T> primary = new Block<>(this.primaryFile.getRecordClass(), this.primaryFile.getBlockSize());
            this.primaryFile.writeBlockToFile(primary, oldHead);
            this.bucketPointers.set(bucketToSplit, oldHead);
        }

        // redistribute records using i+1 bits
        for (T rec : all) {
            long k = this.keyExtractor.apply(rec);
            long h = k & 0x7FFFFFFFFFFFFFFFL;
            long target = h & ((1 << (this.i + 1)) - 1); // mod 2^(i+1)
            if (target == bucketToSplit) {
                this.insertIntoBucketNoSplit(bucketToSplit, rec);
            } else {
                this.insertIntoBucketNoSplit(newBucketIndex, rec);
            }
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
            int newIdx = this.primaryFile.insertRecordWithMetadata(record, head,-1, -1).blockIndex;
            this.bucketPointers.set(bucket, newIdx);
            return;
        }
        int idx = head;
        while (true) {
            Block<T> b = this.primaryFile.getBlock(idx);
            if (b.getValidCount() < b.getBlockFactor()) {
                b.addRecord(record);
                this.primaryFile.writeBlockToFile(b, idx);
                return;
            }
            int next = b.getNextBlockIndex();
            if (next == -1) {
                int newIdx = this.overflowFile.allocateEmptyBlock();
                Block<T> nb = this.overflowFile.getBlock(newIdx);
                b.setNextBlockIndex(newIdx);
                nb.setPreviousBlockIndex(idx);
                this.primaryFile.writeBlockToFile(b, idx);
                nb.addRecord(record);
                this.primaryFile.writeBlockToFile(nb, newIdx);
                return;
            }
            idx = next;
        }
    }

    public int getNumberOfBuckets() {
        return this.bucketPointers.size();
    }

    public Block<T> getBucket(int i) {
        return this.primaryFile.getBlock(this.bucketPointers.get(i));
    }

    public HeapFile<T> getPrimaryFile() {
        return this.primaryFile;
    }

    public HeapFile<T> getOverflowFile() {
        return this.overflowFile;
    }

    public int getBucketRecordCount(int bucket) {
        int count = 0;
        int headBlockIndex = this.bucketPointers.get(bucket);
        if (headBlockIndex == -1) {
            return 0;
        }

        int currentIndex = headBlockIndex;
        while (currentIndex != -1) {
            Block<T> block = this.getFileForBlockIndex(currentIndex).getBlock(currentIndex);
            System.out.println("Bucket: " + bucket + ", Block Index: " + currentIndex + ", Valid Records: " + block.getValidCount());
            for (int i = 0; i < block.getValidCount(); i++) {
                System.out.println(block.getRecordAt(i).toString());
            }
            count += block.getValidCount();
            currentIndex = this.getFileForBlockIndex(currentIndex).getNextBlockIndex(currentIndex);
        }
        return count;
    }

    public int getBucketPointer(int i) {
        return this.bucketPointers.get(i);
    }
}
