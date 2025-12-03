package DS;

import Interface.IHashable;
import Interface.IRecord;

import java.io.*;
import java.util.*;
import java.util.function.Function;

public class LinearHashFile<T extends IRecord<T> & IHashable> {
    private final HeapFile<T> primaryFile;
    private final HeapFile<T> overflowFile;
    private final Function<T, Long> keyExtractor;

    private int i; //aktualna uroven (u)
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
            this.saveDirectory();
        }
    }

    private int bucketForKey(long key) {
        long h = Math.abs(key);
        int mod = (1 << this.i);
        long bucket = h & (mod - 1); // mod 2^i
        if (bucket < this.nextSplit) {
            bucket = h & (((long) mod << 1) - 1); // mod 2^(i+1)
        }
        return (int) bucket;
    }

    private void saveDirectory() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(this.dirFile))) {
            pw.println(this.i);
            pw.println(this.nextSplit);
        } catch (IOException e) {
            throw new RuntimeException("Error saving hash directory", e);
        }
    }

    private void loadDirectory() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.dirFile))) {
            this.i = Integer.parseInt(br.readLine().trim());
            this.nextSplit = Integer.parseInt(br.readLine().trim());
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

        T found = this.primaryFile.findRecord(bucket, record);
        if (found != null) {
            return found;
        }

        int currentIndex = bucket;
        while (currentIndex != -1) {
            ChainedBlock block = (ChainedBlock) this.primaryFile.getBlock(currentIndex);
            int nextIndex = block.getNextBlockIndex();
            if (nextIndex != -1) {
                return this.overflowFile.findInChain(nextIndex, record);
            }
            currentIndex = nextIndex;
        }

        return null;
    }

    private void insertIntoBucket(int bucket, T record) {
        HeapFile.BlockInsertResult<T> result = this.primaryFile.insertRecordWithMetadata(record, bucket, -1);

        if (result.blockIndex == -1) {
            ChainedBlock b = (ChainedBlock) result.block;
            int currentIndex = bucket;
            // najdenie posledneho bloku v retazci
            while (b.getNextBlockIndex() != -1 && b.getValidCount() == b.getBlockFactor()) {
                currentIndex = b.getNextBlockIndex();
                b = (ChainedBlock) this.overflowFile.getBlock(currentIndex);
            }
            this.overflowFile.insertRecordWithMetadata(record, currentIndex, b.getNextBlockIndex());
        }
        this.splitNextBucketIfNeeded();
    }

    private void splitNextBucketIfNeeded() {
        double loadFactor = (double) (this.primaryFile.getTotalRecords()) / ((this.nextSplit + (1 << this.i)) * this.primaryFile.getBlockFactor());
        if (loadFactor > 0.75) {
            this.splitNextBucket();
        }
    }

    public void splitNextBucket() {
        int bucketToSplit = this.nextSplit;

        List<T> all = new ArrayList<>();

        int index = bucketToSplit;
        ChainedBlock b = (ChainedBlock) this.primaryFile.getBlock(index);
        for (int r = 0; r < b.getValidCount(); r++) {
            IRecord<T> rec = b.getRecordAt(r);
            if (rec != null) {
                all.add(rec.createCopy());
            }
        }
        index = b.getNextBlockIndex();
        while (index != -1) {
            b = (ChainedBlock) this.overflowFile.getBlock(index);
            for (int r = 0; r < b.getValidCount(); r++) {
                IRecord<T> rec = b.getRecordAt(r);
                if (rec != null) {
                    all.add(rec.createCopy());
                }
            }
            b.setValidCount(0);
            this.overflowFile.updateListsAfterDelete(index, b);
            index = b.getNextBlockIndex();
        }
        this.overflowFile.trimTrailingEmptyBlocks();

        int newBucketIndex = bucketToSplit + (1 << this.i);

        //rehashovanie a vloženie záznamov do správnych bucketov
        LinkedList<T> oldBacketRecords = new LinkedList<>();
        LinkedList<T> newBacketRecords = new LinkedList<>();
        for (T rec : all) {
            long k = this.keyExtractor.apply(rec);
            long h = Math.abs(k);
            int mod = (1 << this.i);
            long target = h & (((long) mod << 1) - 1); // mod 2^(i+1)
            if (target == bucketToSplit) {
                oldBacketRecords.add(rec);
            } else {
                newBacketRecords.add(rec);
            }
        }

        if(!oldBacketRecords.isEmpty()) {
            this.insertIntoBucketNoSplit(bucketToSplit, oldBacketRecords);
        }
        if(!newBacketRecords.isEmpty()) {
            this.insertIntoBucketNoSplit(newBucketIndex, newBacketRecords);
        }

        this.nextSplit++;
        if (this.nextSplit >= (1 << this.i)) {
            this.nextSplit = 0;
            this.i++;
        }
        this.splitNextBucketIfNeeded();
        this.saveDirectory();
    }

    private void insertIntoBucketNoSplit(int bucket, LinkedList<T> records) {
        List<Block<T>> chain = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        List<Boolean> isPrimaryList = new ArrayList<>();

        int currentIndex = bucket;
        boolean isPrimary = true;
        while (currentIndex != -1) {
            ChainedBlock currentBlock = (ChainedBlock) (isPrimary ? this.primaryFile.getBlock(currentIndex) : this.overflowFile.getBlock(currentIndex));
            chain.add(currentBlock);
            indices.add(currentIndex);
            isPrimaryList.add(isPrimary);
            currentIndex = currentBlock.getNextBlockIndex();
            isPrimary = false;
        }

        for (int i = 0; i < chain.size() && !records.isEmpty(); i++) {
            Block<T> block = chain.get(i);
            int index = indices.get(i);
            boolean blockIsPrimary = isPrimaryList.get(i);

            while (block.getValidCount() < block.getBlockFactor() && !records.isEmpty()) {
                block.addRecord(records.removeFirst());
            }

            if (blockIsPrimary) {
                this.primaryFile.writeBlockToFile(block, index);
            } else {
                this.overflowFile.writeBlockToFile(block, index);
            }

            if (records.isEmpty()) {
                return;
            }
        }

        int lastIndex = indices.getLast();
        boolean lastIsPrimary = isPrimaryList.getLast();
        ChainedBlock lastBlock = (ChainedBlock) chain.getLast();

        while (!records.isEmpty()) {
            HeapFile.BlockInsertResult<T> res = this.overflowFile.insertRecordAsNewBlock(records.removeFirst(), -1);
            while (res.block.getValidCount() < res.block.getBlockFactor() && !records.isEmpty()) {
                res.block.addRecord(records.removeFirst());
            }
            this.overflowFile.writeBlockToFile(res.block, res.blockIndex);

            lastBlock.setNextBlockIndex(res.blockIndex);
            if (lastIsPrimary) {
                this.primaryFile.writeBlockToFile(lastBlock, lastIndex);
            } else {
                this.overflowFile.writeBlockToFile(lastBlock, lastIndex);
            }

            lastIndex = res.blockIndex;
            lastIsPrimary = false;
            lastBlock = (ChainedBlock) res.block;
        }
    }

    public Block<T> getBucket(int i) {
        return this.primaryFile.getBlock(i);
    }

    public HeapFile<T> getPrimaryFile() {
        return this.primaryFile;
    }

    public HeapFile<T> getOverflowFile() {
        return this.overflowFile;
    }

    public int getBucketRecordCount(int bucket) {
        int count = 0;

        int currentIndex = bucket;
        ChainedBlock block = (ChainedBlock) this.primaryFile.getBlock(currentIndex);
        System.out.println("Bucket: " + bucket + ", Block Index: " + currentIndex + ", Valid Records: " + block.getValidCount());
        for (int i = 0; i < block.getValidCount(); i++) {
            System.out.println(block.getRecordAt(i).toString());
        }
        count += block.getValidCount();
        currentIndex = this.primaryFile.getNextBlockIndex(currentIndex);
        while (currentIndex != -1) {
            ChainedBlock b = (ChainedBlock) this.overflowFile.getBlock(currentIndex);
            System.out.println("Bucket: " + bucket + ", Overflow Block Index: " + currentIndex + ", Valid Records: " + b.getValidCount());
            for (int i = 0; i < b.getValidCount(); i++) {
                System.out.println(b.getRecordAt(i).toString());
            }
            count += b.getValidCount();
            currentIndex = b.getNextBlockIndex();
        }
        return count;
    }
}
