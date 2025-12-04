package DS;

import Interface.IHashable;
import Interface.IRecord;

import java.io.*;
import java.util.*;
import java.util.function.Function;

public class LinearHashFile<T extends IRecord<T> & IHashable> {
    private final HeapFile<ChainedBlock<T>, T> primaryFile;
    private final HeapFile<ChainedBlock<T>, T> overflowFile;
    private final Function<T, Long> keyExtractor;

    private int i; //aktualna uroven (u)
    private int nextSplit;
    private final File dirFile;

    public LinearHashFile(Class<T> recordClass, int initialBuckets, Function<T,Long> keyExtractor, String primaryFileName, String overflowFileName, int blockSizePrimary, int blockSizeOverflow) {
        if (initialBuckets <= 0 || (initialBuckets & (initialBuckets - 1)) != 0) {
            throw new IllegalArgumentException("initialBuckets must be power of two and > 0");
        }

        @SuppressWarnings("unchecked")
        Class<ChainedBlock<T>> chainedBlockClass = (Class<ChainedBlock<T>>) (Class<?>) ChainedBlock.class;
        this.primaryFile = new HeapFile<>(primaryFileName, recordClass, chainedBlockClass ,blockSizePrimary);
        this.overflowFile = new HeapFile<>(overflowFileName, recordClass,chainedBlockClass, blockSizeOverflow);
        this.keyExtractor = keyExtractor;
        this.dirFile = new File(primaryFileName + "_dir.txt");

        this.i = Integer.numberOfTrailingZeros(initialBuckets);
        this.nextSplit = 0;

        if (this.dirFile.exists()) {
            this.loadDirectory();
        } else {
            this.primaryFile.alocateBlocks(initialBuckets);
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

        ChainedBlock block = this.primaryFile.getBlock(bucket);
        for (int r = 0; r < block.getValidCount(); r++) {
            IRecord<T> rec = block.getRecordAt(r);
            if (rec != null && rec.isEqual(record)) {
                return rec.createCopy();
            }
        }
        int nextIndex = block.getNextBlockIndex();
        if (nextIndex != -1) {
            return this.overflowFile.findInChain(nextIndex, record);
        }
        return null;
    }

    private void insertIntoBucket(int bucket, T record) {
        HeapFile.BlockInsertResult<T> result = this.primaryFile.insertRecordWithMetadata(record, bucket);

        if (result.blockIndex == -1) {
            ChainedBlock b = (ChainedBlock) result.block;
            int currentIndex = bucket;
            boolean isPrimary = true;
            // najdenie posledneho bloku v retazci
            while (b.getNextBlockIndex() != -1 && b.getValidCount() == b.getBlockFactor()) {
                currentIndex = b.getNextBlockIndex();
                b = this.overflowFile.getBlock(currentIndex);
                isPrimary = false;
            }
            HeapFile.BlockInsertResult<T> newResult = this.overflowFile.insertRecordWithMetadata(record, currentIndex);
            b.setNextBlockIndex(newResult.blockIndex);
            if (newResult.blockIndex == -1) {
                HeapFile.BlockInsertResult<T> finalResult = this.overflowFile.insertRecordAsNewBlock(record);
                b.setNextBlockIndex(finalResult.blockIndex);
            }
            if (isPrimary) {
                this.primaryFile.writeBlockToFile(b, bucket);
            } else {
                this.overflowFile.writeBlockToFile(b, currentIndex);
            }

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
        ArrayList<ChainedBlock<T>> chainedBlocks = new ArrayList<>();
        int index = bucketToSplit;
        ChainedBlock b = this.primaryFile.getBlock(index);
        for (int r = 0; r < b.getValidCount(); r++) {
            IRecord<T> rec = b.getRecordAt(r);
            if (rec != null) {
                all.add(rec.createCopy());
            }
        }
        chainedBlocks.add(b);
        index = b.getNextBlockIndex();
        while (index != -1) {
            b = this.overflowFile.getBlock(index);
            for (int r = 0; r < b.getValidCount(); r++) {
                IRecord<T> rec = b.getRecordAt(r);
                if (rec != null) {
                    all.add(rec.createCopy());
                }
            }
            index = b.getNextBlockIndex();
            chainedBlocks.add(b);
        }
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


        this.insertIntoOldBucketNoSplit(bucketToSplit, chainedBlocks, oldBacketRecords);
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

    private void insertIntoOldBucketNoSplit(int blockIndex, ArrayList<ChainedBlock<T>> oldChain, LinkedList<T> oldBacketRecords) {
        if (oldBacketRecords.isEmpty()) {
            this.updateBucketValidCount(blockIndex, oldChain);
            return;
        }
        int previousPrimaryRecords = oldChain.getFirst().getValidCount();
        int previousOverflowRecords = 0;
        for(int j =1; j< oldChain.size(); j++){
            previousOverflowRecords += oldChain.get(j).getValidCount();
        }
        ArrayList<Integer> nextBLockPointers = new ArrayList<>();
        for (int j = 0; j < oldChain.size(); j++) {
            oldChain.get(j).setValidCount(0);
            if (j > 0) {
                this.overflowFile.updateListsAfterDelete(nextBLockPointers.get(j - 1), oldChain.get(j));
            }
            nextBLockPointers.add(oldChain.get(j).getNextBlockIndex());
            oldChain.get(j).setNextBlockIndex(-1);
        }
        int newPrimaryRecords = 0;
        int newOverflowRecords = 0;
        while (oldChain.getFirst().getValidCount() < oldChain.getFirst().getBlockFactor() && !oldBacketRecords.isEmpty()) {
            oldChain.getFirst().addRecord(oldBacketRecords.removeFirst());
            newPrimaryRecords++;
        }
        int i = 1;
        if (oldChain.size() > 1) {
            while (oldChain.get(i).getValidCount() < oldChain.get(i).getBlockFactor() && !oldBacketRecords.isEmpty()) {
                oldChain.get(i).addRecord(oldBacketRecords.removeFirst());
                newOverflowRecords++;
                if (oldChain.get(i).getValidCount() == oldChain.get(i).getBlockFactor()) {
                    i++;
                }
            }
        }
        if (oldChain.size() > 1) {
            if (oldChain.get(1).getValidCount() < oldChain.get(1).getBlockFactor()) {
                oldChain.getFirst().setNextBlockIndex(nextBLockPointers.getFirst());
            }
        }
        this.primaryFile.writeBlockToFile(oldChain.getFirst(), blockIndex);
        for (int j = 1; j < oldChain.size(); j++) {
            if (oldChain.get(j).getValidCount() < oldChain.get(j).getBlockFactor()) {
                oldChain.get(j).setNextBlockIndex(nextBLockPointers.get(j));
            }
            this.overflowFile.writeBlockToFile(oldChain.get(j), nextBLockPointers.get(j - 1));
        }
        int totalRecords = this.primaryFile.getTotalRecords();
        if (newPrimaryRecords > previousPrimaryRecords) {
            this.primaryFile.setTotalRecords(totalRecords + (previousPrimaryRecords - newPrimaryRecords));
        }
        totalRecords = this.overflowFile.getTotalRecords();
        if (newOverflowRecords < previousOverflowRecords) {
            this.overflowFile.setTotalRecords(totalRecords - (previousOverflowRecords - newOverflowRecords));
        }
        this.overflowFile.trimTrailingEmptyBlocks();
        this.primaryFile.saveHeader();
        this.overflowFile.saveHeader();
    }

    private void updateBucketValidCount(int blockIndex, ArrayList<ChainedBlock<T>> oldChain) {
        ArrayList<Integer> nextBLockPointers = new ArrayList<>();
        for (int j = 0; j < oldChain.size(); j++) {
            oldChain.get(j).setValidCount(0);
            if (j > 0) {
                this.overflowFile.updateListsAfterDelete(nextBLockPointers.get(j - 1), oldChain.get(j));
            }
            nextBLockPointers.add(oldChain.get(j).getNextBlockIndex());
            oldChain.get(j).setNextBlockIndex(-1);
        }
        this.primaryFile.writeBlockToFile(oldChain.getFirst(), blockIndex);
        if (oldChain.size() > 1) {
            for (int i = 0; i < nextBLockPointers.size(); i++) {
                this.overflowFile.writeBlockToFile(oldChain.getFirst(), nextBLockPointers.get(i));
            }
        }
        this.overflowFile.trimTrailingEmptyBlocks();
    }

    private void insertIntoBucketNoSplit(int bucket, LinkedList<T> records) {
        ChainedBlock<T> block = new ChainedBlock<>(this.primaryFile.getRecordClass(), this.primaryFile.getBlockSize());
        this.primaryFile.incrementTotalBlocks();
        int newPrimaryRecords = 0;
        while (block.getValidCount() < block.getBlockFactor() && !records.isEmpty()) {
            block.addRecord(records.removeFirst());
            newPrimaryRecords++;
        }
        if (records.isEmpty()) {
            this.primaryFile.writeBlockToFile(block, bucket);
            this.primaryFile.setTotalRecords(this.primaryFile.getTotalRecords() + newPrimaryRecords);
            this.primaryFile.saveHeader();
            return;
        }
        ChainedBlock<T> lastBlock = block;
        int lastBlockIndex = bucket;
        boolean lastIsPrimary = true;
        int newOverflowRecords = 0;
        while (!records.isEmpty()) {
            ChainedBlock<T> overflowBlock = new ChainedBlock<>(this.overflowFile.getRecordClass(), this.overflowFile.getBlockSize());
            while (overflowBlock.getValidCount() < overflowBlock.getBlockFactor() && !records.isEmpty()) {
                overflowBlock.addRecord(records.removeFirst());
                newOverflowRecords++;
            }
            int overflowBlockIndex = this.overflowFile.getTotalBlocks();
            this.overflowFile.writeBlockToFile(overflowBlock, overflowBlockIndex);
            this.overflowFile.incrementTotalBlocks();
            this.overflowFile.saveHeader();
            lastBlock.setNextBlockIndex(overflowBlockIndex);
            if (lastIsPrimary) {
                this.primaryFile.saveHeader();
                this.primaryFile.writeBlockToFile(lastBlock, lastBlockIndex);
            } else {
                this.overflowFile.writeBlockToFile(lastBlock, lastBlockIndex);
            }
            lastBlock = overflowBlock;
            lastBlockIndex = overflowBlockIndex;
            lastIsPrimary = false;
        }
        this.primaryFile.setTotalRecords(this.primaryFile.getTotalRecords() + newPrimaryRecords);
        this.overflowFile.setTotalRecords(this.overflowFile.getTotalRecords() + newOverflowRecords);
        this.primaryFile.saveHeader();
        this.overflowFile.saveHeader();
    }

    public HeapFile<ChainedBlock<T>,T> getPrimaryFile() {
        return this.primaryFile;
    }

    public HeapFile<ChainedBlock<T>,T> getOverflowFile() {
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
