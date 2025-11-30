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

    private List<Integer> bucketPointers;
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
            this.bucketPointers = new ArrayList<>();
            for (int b = 0; b < initialBuckets; b++) {
                this.bucketPointers.add(-1);
            }
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

        T found = this.primaryFile.findRecord(headBlockIndex, record);
        if (found != null) {
            return found;
        }

        int currentIndex = headBlockIndex;
        while (currentIndex != -1) {
            Block<T> block = this.primaryFile.getBlock(currentIndex);
            int nextIndex = block.getNextBlockIndex();
            if (nextIndex != -1) {
                return this.overflowFile.findInChain(nextIndex, record);
            }
            currentIndex = nextIndex;
        }

        return null;
    }

    private void insertIntoBucket(int bucket, T record) {
        int headBlockIndex = this.bucketPointers.get(bucket);

        if (headBlockIndex == -1) {
            HeapFile.BlockInsertResult<T> result = this.primaryFile.insertRecordAsNewBlock(record, -1);
            this.bucketPointers.set(bucket, result.blockIndex);
            this.saveDirectory();
            return;
        }
        System.out.println("Insert into bucket " + bucket + ", head block index: " + headBlockIndex);
        HeapFile.BlockInsertResult<T> result = this.primaryFile.insertRecordWithMetadata(record, headBlockIndex, -1);

        if (result.blockIndex == -1) {
            Block<T> b = result.block;
            int currentIndex = headBlockIndex;
            boolean lastIsPrimary = true;
            // najdenie posledneho bloku v retazci
            while (b.getNextBlockIndex() != -1 && b.getValidCount() == b.getBlockFactor()) {
                currentIndex = b.getNextBlockIndex();
                b = this.overflowFile.getBlock(currentIndex);
                lastIsPrimary = false;
            }

            if (b.getValidCount() < b.getBlockFactor()) {
                //ak je miesto vlozim
                this.overflowFile.insertRecordWithMetadata(record, currentIndex, b.getNextBlockIndex());
            } else {
                //nie je miesto, vytvorim novy overflow block
                this.addNewOverflowBlock(currentIndex, record,b, lastIsPrimary);
            }
        }

        this.splitNextBucketIfNeeded();
    }

    private void addNewOverflowBlock(int chainHeadIndex, T record,Block<T> previousBlock, boolean lastIsPrimary) {
        HeapFile.BlockInsertResult<T> res = this.overflowFile.insertRecordAsNewBlock(record, -1);
        int newBlockIndex = res.blockIndex;

        previousBlock.setNextBlockIndex(newBlockIndex);
        if (lastIsPrimary) {
            this.primaryFile.writeBlockToFile(previousBlock, chainHeadIndex);
        } else {
            this.overflowFile.writeBlockToFile(previousBlock, chainHeadIndex);
        }
    }

    private void splitNextBucketIfNeeded() {
        double loadFactor = (double) (this.primaryFile.getTotalRecords()) / (this.getNumberOfBuckets() * this.primaryFile.getBlockFactor());
        if (loadFactor > 0.75) {
            this.splitNextBucket();
        }
    }

    public void splitNextBucket() {
        int bucketToSplit = this.nextSplit;
        int oldHead = this.bucketPointers.get(bucketToSplit);

        List<T> all = new ArrayList<>();
        if (oldHead != -1) {
            int index = oldHead;
            Block<T> b = this.primaryFile.getBlock(index);
            for (int r = 0; r < b.getValidCount(); r++) {
                IRecord<T> rec = b.getRecordAt(r);
                if (rec != null) {
                    all.add(rec.createCopy());
                }
            }
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
            }
        }

        int newBucketIndex = this.bucketPointers.size();
        this.bucketPointers.add(-1);

        //ak nebol prazdny bucket, prepíšem ho na prázdny
        if (oldHead != -1) {
            Block<T> primary = new Block<>(this.primaryFile.getRecordClass(), this.primaryFile.getBlockSize());
            this.primaryFile.writeBlockToFile(primary, oldHead);
            this.bucketPointers.set(bucketToSplit, oldHead);
        }

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
        int head = this.bucketPointers.get(bucket);
        if (head == -1) {
            HeapFile.BlockInsertResult<T> result = this.primaryFile.insertRecordAsNewBlock(records.removeFirst(), -1);
            this.bucketPointers.set(bucket, result.blockIndex);
            while (result.block.getValidCount() < result.block.getBlockFactor() && !records.isEmpty()) {
                result.block.addRecord(records.removeFirst());
            }
            this.primaryFile.writeBlockToFile(result.block, result.blockIndex);
            int lastIndex = result.blockIndex;
            boolean lastIsPrimary = true;
            while (!records.isEmpty()) {
                HeapFile.BlockInsertResult<T> res = this.overflowFile.insertRecordAsNewBlock(records.removeFirst(), -1);
                while (res.block.getValidCount() < res.block.getBlockFactor() && !records.isEmpty()) {
                    res.block.addRecord(records.removeFirst());
                }
                this.overflowFile.writeBlockToFile(res.block, res.blockIndex);
                Block<T> prevBlock = lastIsPrimary ? this.primaryFile.getBlock(lastIndex) : this.overflowFile.getBlock(lastIndex);
                prevBlock.setNextBlockIndex(res.blockIndex);
                if (lastIsPrimary) {
                    this.primaryFile.writeBlockToFile(prevBlock, lastIndex);
                } else {
                    this.overflowFile.writeBlockToFile(prevBlock, lastIndex);
                }
                lastIndex = res.blockIndex;
                lastIsPrimary = false;
            }
            this.saveDirectory();
            return;
        }

        List<Block<T>> chain = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        List<Boolean> isPrimaryList = new ArrayList<>();

        int currentIndex = head;
        boolean isPrimary = true;
        while (currentIndex != -1) {
            Block<T> currentBlock = isPrimary ? this.primaryFile.getBlock(currentIndex) : this.overflowFile.getBlock(currentIndex);
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
        Block<T> lastBlock = chain.getLast();

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
            lastBlock = res.block;
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
        Block<T> block = this.primaryFile.getBlock(currentIndex);
        System.out.println("Bucket: " + bucket + ", Block Index: " + currentIndex + ", Valid Records: " + block.getValidCount());
        for (int i = 0; i < block.getValidCount(); i++) {
            System.out.println(block.getRecordAt(i).toString());
        }
        count += block.getValidCount();
        currentIndex = this.primaryFile.getNextBlockIndex(currentIndex);
        while (currentIndex != -1) {
            Block<T> b = this.overflowFile.getBlock(currentIndex);
            System.out.println("Bucket: " + bucket + ", Overflow Block Index: " + currentIndex + ", Valid Records: " + b.getValidCount());
            for (int i = 0; i < b.getValidCount(); i++) {
                System.out.println(b.getRecordAt(i).toString());
            }
            count += b.getValidCount();
            currentIndex = b.getNextBlockIndex();
        }
        return count;
    }

    public int getBucketPointer(int i) {
        return this.bucketPointers.get(i);
    }
}
