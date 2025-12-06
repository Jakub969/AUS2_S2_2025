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

    private String baseFolder;

    public LinearHashFile(Class<T> recordClass, int initialBuckets,
                          Function<T, Long> keyExtractor,
                          String folderPath, int blockSizePrimary, int blockSizeOverflow) {

        if (initialBuckets <= 0 || (initialBuckets & (initialBuckets - 1)) != 0) {
            throw new IllegalArgumentException("initialBuckets must be power of two and > 0");
        }

        this.baseFolder = folderPath;
        this.keyExtractor = keyExtractor;

        File folder = new File(folderPath);
        if (!folder.exists()) {
            folder.mkdirs();
        }

        String primaryFileName = folderPath + File.separator + "primary_data.bin";
        String overflowFileName = folderPath + File.separator + "overflow_data.bin";
        String dirFileName = folderPath + File.separator + "directory.txt";

        Class<ChainedBlock<T>> chainedBlockClass = (Class<ChainedBlock<T>>) (Class<?>) ChainedBlock.class;

        this.primaryFile = new HeapFile<>(primaryFileName, recordClass, chainedBlockClass, blockSizePrimary);
        this.overflowFile = new HeapFile<>(overflowFileName, recordClass, chainedBlockClass, blockSizeOverflow);
        this.dirFile = new File(dirFileName);

        this.i = Integer.numberOfTrailingZeros(initialBuckets);
        this.nextSplit = 0;

        if (this.dirFile.exists()) {
            this.loadDirectory();
        } else {
            this.primaryFile.alocateBlocks(initialBuckets);
            this.saveDirectory();
        }
    }

    public LinearHashFile(Class<T> recordClass, int initialBuckets, Function<T,Long> keyExtractor, String primaryFileName, String overflowFileName, int blockSizePrimary, int blockSizeOverflow) {
        if (initialBuckets <= 0 || (initialBuckets & (initialBuckets - 1)) != 0) {
            throw new IllegalArgumentException("initialBuckets must be power of two and > 0");
        }

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

    public void edit(T newRecord) {
        long key = this.keyExtractor.apply(newRecord);
        int bucket = this.bucketForKey(key);
        ChainedBlock block = this.primaryFile.getBlock(bucket);
        for (int r = 0; r < block.getValidCount(); r++) {
            IRecord<T> rec = block.getRecordAt(r);
            if (rec != null && rec.isEqual(newRecord)) {
                block.updateRecordAt(r, newRecord);
                this.primaryFile.writeBlockToFile(block, bucket);
                return;
            }
        }
        int nextIndex = block.getNextBlockIndex();
        if (nextIndex != -1) {
            boolean updated = this.overflowFile.editInChain(nextIndex, newRecord);
            if (updated) {
                return;
            }
        }
        throw new NoSuchElementException("Record to edit not found.");
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
            if (b.getValidCount() == b.getBlockFactor()) {
                HeapFile.BlockInsertResult<T> newResult = this.overflowFile.insertRecordAsNewBlock(record);
                b.setNextBlockIndex(newResult.blockIndex);
                if (isPrimary) {
                    this.primaryFile.writeBlockToFile(b, bucket);
                } else {
                    this.overflowFile.writeBlockToFile(b, currentIndex);
                }
            } else {
                this.overflowFile.insertRecordWithMetadata(record, currentIndex);
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

        ArrayList<Integer> pointers = this.insertIntoOldBucketNoSplit(bucketToSplit, chainedBlocks, oldBacketRecords);
        this.insertIntoBucketNoSplit(newBucketIndex, chainedBlocks, pointers, newBacketRecords);

        this.nextSplit++;
        if (this.nextSplit >= (1 << this.i)) {
            this.nextSplit = 0;
            this.i++;
        }
        this.splitNextBucketIfNeeded();
        this.saveDirectory();
    }

    private ArrayList<Integer> insertIntoOldBucketNoSplit(int blockIndex, ArrayList<ChainedBlock<T>> oldChain, LinkedList<T> oldBacketRecords) {
        int previousPrimaryRecords = oldChain.getFirst().getValidCount();
        int previousOverflowRecords = 0;
        for(int j =1; j< oldChain.size(); j++){
            previousOverflowRecords += oldChain.get(j).getValidCount();
        }
        ArrayList<Integer> nextBLockPointers = new ArrayList<>();
        for (int j = 0; j < oldChain.size(); j++) {
            nextBLockPointers.add(oldChain.get(j).getNextBlockIndex());
            oldChain.get(j).setValidCount(0);
            oldChain.get(j).setNextBlockIndex(-1);
            if (j > 0) {
                this.overflowFile.updateListsAfterDelete(nextBLockPointers.get(j - 1), oldChain.get(j));
            }
        }
        int newPrimaryRecords = 0;
        int newOverflowRecords = 0;
        while (oldChain.getFirst().getValidCount() < oldChain.getFirst().getBlockFactor() && !oldBacketRecords.isEmpty()) {
            oldChain.getFirst().addRecord(oldBacketRecords.removeFirst());
            newPrimaryRecords++;
        }
        int i = 1;
        if (oldChain.size() > 1) {
            while (i < oldChain.size() && oldChain.get(i).getValidCount() < oldChain.get(i).getBlockFactor() && !oldBacketRecords.isEmpty()) {
                oldChain.get(i).addRecord(oldBacketRecords.removeFirst());
                newOverflowRecords++;
                this.overflowFile.updateListsAfterInsert(nextBLockPointers.get(i - 1), oldChain.get(i));
                if (oldChain.get(i).getValidCount() == oldChain.get(i).getBlockFactor()) {
                    i++;
                }
            }
        }
        int lastUsedIndex = -1;
        if (newPrimaryRecords > 0) {
            lastUsedIndex = 0;
        }
        for (int j = 1; j < oldChain.size(); j++) {
            if (oldChain.get(j).getValidCount() > 0) {
                lastUsedIndex = j;
            }
        }
        for (int j = 0; j <= lastUsedIndex; j++) {
            if (j == lastUsedIndex) {
                oldChain.get(j).setNextBlockIndex(-1);
            } else {
                int nextBlockFileIndex;
                nextBlockFileIndex = nextBLockPointers.get(j);
                oldChain.get(j).setNextBlockIndex(nextBlockFileIndex);
            }
        }
        this.primaryFile.writeBlockToFile(oldChain.getFirst(), blockIndex);
        for (int j = 1; j < oldChain.size(); j++) {
            if(oldChain.get(j).getValidCount() > 0) {
                this.overflowFile.writeBlockToFile(oldChain.get(j), nextBLockPointers.get(j - 1));
            }
        }
        ArrayList<ChainedBlock<T>> unusedBlocks = new ArrayList<>();
        for (int j = 1; j < oldChain.size(); j++) {
            if (oldChain.get(j).getValidCount() == 0) {
                unusedBlocks.add(oldChain.get(j));
            } else {
                nextBLockPointers.removeFirst();
            }
        }
        oldChain.clear();
        oldChain.addAll(unusedBlocks);
        int primaryDiff = newPrimaryRecords - previousPrimaryRecords;
        this.primaryFile.setTotalRecords(this.primaryFile.getTotalRecords() + primaryDiff);

        int overflowDiff = newOverflowRecords - previousOverflowRecords;
        this.overflowFile.setTotalRecords(this.overflowFile.getTotalRecords() + overflowDiff);
        return nextBLockPointers;
    }

    private void insertIntoBucketNoSplit(int bucket, ArrayList<ChainedBlock<T>> oldChain, ArrayList<Integer> pointers, LinkedList<T> records) {
        ChainedBlock<T> block = new ChainedBlock<>(this.primaryFile.getRecordClass(), this.primaryFile.getBlockSize());
        int newPrimaryRecords = 0;
        while (block.getValidCount() < block.getBlockFactor() && !records.isEmpty()) {
            block.addRecord(records.removeFirst());
            newPrimaryRecords++;
        }
        if (records.isEmpty() && newPrimaryRecords > 0) {
            this.primaryFile.incrementTotalBlocks();
            this.primaryFile.writeBlockToFile(block, bucket);
            this.primaryFile.setTotalRecords(this.primaryFile.getTotalRecords() + newPrimaryRecords);
            this.primaryFile.saveHeader();
            if (!oldChain.isEmpty()) {
                for (int j = 0; j < oldChain.size(); j++) {
                    this.overflowFile.writeBlockToFile(oldChain.get(j), pointers.get(j));
                }
            }
            this.overflowFile.trimTrailingEmptyBlocks();
            this.overflowFile.saveHeader();
            return;
        } else if (newPrimaryRecords == 0) {
            this.primaryFile.incrementTotalBlocks();
            this.primaryFile.writeBlockToFile(block, bucket);
            this.primaryFile.saveHeader();
            this.overflowFile.trimTrailingEmptyBlocks();
            this.overflowFile.saveHeader();
            return;
        }
        ChainedBlock<T> lastBlock = block;
        int lastBlockIndex = bucket;
        boolean lastIsPrimary = true;
        int overflowBlocksUsed = 0;
        int newOverflowRecords = 0;
        int i = 0;
        while (!records.isEmpty()) {
            while (oldChain.get(i).getValidCount() < oldChain.get(i).getBlockFactor() && !records.isEmpty()) {
                oldChain.get(i).addRecord(records.removeFirst());
                newOverflowRecords++;
            }
            this.overflowFile.updateListsAfterInsert(pointers.get(i), oldChain.get(i));
            overflowBlocksUsed++;
            this.overflowFile.writeBlockToFile(oldChain.get(i), pointers.get(i));
            lastBlock.setNextBlockIndex(pointers.get(i));
            if (lastIsPrimary) {
                this.primaryFile.incrementTotalBlocks();
                this.primaryFile.writeBlockToFile(lastBlock, lastBlockIndex);
            } else {
                this.overflowFile.writeBlockToFile(lastBlock, lastBlockIndex);
            }
            lastBlock = oldChain.get(i);
            lastBlockIndex = pointers.get(i);
            lastIsPrimary = false;
            i++;
        }
        if (!oldChain.isEmpty()) {
            for (int j = overflowBlocksUsed; j < oldChain.size(); j++) {
                this.overflowFile.writeBlockToFile(oldChain.get(j), pointers.get(j));
            }
        }
        this.primaryFile.setTotalRecords(this.primaryFile.getTotalRecords() + newPrimaryRecords);
        this.overflowFile.setTotalRecords(this.overflowFile.getTotalRecords() + newOverflowRecords);
        this.overflowFile.trimTrailingEmptyBlocks();
        this.primaryFile.saveHeader();
        this.overflowFile.saveHeader();
    }

    public void close() {
        this.saveDirectory();
        this.primaryFile.close();
        this.overflowFile.close();
    }

    public String getFolderPath() {
        return this.baseFolder;
    }

    public HeapFile<ChainedBlock<T>,T> getPrimaryFile() {
        return this.primaryFile;
    }

    public HeapFile<ChainedBlock<T>,T> getOverflowFile() {
        return this.overflowFile;
    }
    //používa sa iba pri testovaní
    public int getBucketRecordCount(int bucket) {
        int count = 0;

        int currentIndex = bucket;
        ChainedBlock block = this.primaryFile.getBlock(currentIndex);
        System.out.println("Bucket: " + bucket + ", Block Index: " + currentIndex + ", Valid Records: " + block.getValidCount());
        for (int i = 0; i < block.getValidCount(); i++) {
            System.out.println(block.getRecordAt(i).toString());
        }
        count += block.getValidCount();
        currentIndex = this.primaryFile.getNextBlockIndex(currentIndex);
        while (currentIndex != -1) {
            ChainedBlock b = this.overflowFile.getBlock(currentIndex);
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
