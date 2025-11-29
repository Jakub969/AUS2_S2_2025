package DS;

import Interface.IRecord;

import java.io.*;
import java.util.*;

public class HeapFile<T extends IRecord<T>> {
    private final File dataFile;
    private final File emptyBlocksFile;
    private final File partialBlocksFile;
    private final File headerFile;
    private final Class<T> recordClass;
    private final int blockSize;
    private final LinkedList<Integer> emptyBlocks;
    private final LinkedList<Integer> partiallyEmptyBlocks;
    private final List<Integer> blockValidCounts;
    private int totalBlocks;
    private int totalRecords;

    public HeapFile(String baseFileName, Class<T> recordClass, int blockSize) {
        this.dataFile = new File(baseFileName);
        this.emptyBlocksFile = new File(baseFileName + "_empty.txt");
        this.partialBlocksFile = new File(baseFileName + "_partial.txt");
        this.headerFile = new File(baseFileName + "_header.txt");
        this.recordClass = recordClass;
        this.blockSize = blockSize;
        this.emptyBlocks = new LinkedList<>();
        this.partiallyEmptyBlocks = new LinkedList<>();
        this.blockValidCounts = new LinkedList<>();
        if (this.dataFile.exists()) {
            this.loadLists();
            this.loadHeader();
        } else {
            this.saveHeader();
            this.saveLists();
        }
    }

    public BlockInsertResult insertRecord(T record, int blockindex) {
        if (!this.partiallyEmptyBlocks.isEmpty()) {
            this.partiallyEmptyBlocks.remove(Integer.valueOf(blockindex));
        } else if (!this.emptyBlocks.isEmpty()) {
            this.emptyBlocks.remove(Integer.valueOf(blockindex));
        }

        if (blockindex > this.totalBlocks) {
            //block = new Block<>(this.recordClass, this.blockSize);
            return new BlockInsertResult(-1, null); // Indikácia, že blok je plný a nie je možné vložiť záznam
        }
        return new BlockInsertResult(blockindex, this.getBlock(blockindex));
    }

    public BlockInsertResult insertRecordWithMetadata(T record,int blockIndex, int nextBlock, int prevBlock) {
        BlockInsertResult insertResult = this.insertRecord(record, blockIndex);
        if (insertResult.blockIndex == -1) {
            return insertResult;
        }
        insertResult.block.setNextBlockIndex(nextBlock);
        insertResult.block.setPreviousBlockIndex(prevBlock);
        insertResult.block.addRecord(record);
        this.updateListsAfterInsert(insertResult.blockIndex, insertResult.block);
        this.writeBlockToFile(insertResult.block, insertResult.blockIndex);
        if (insertResult.blockIndex == this.totalBlocks) {
            this.totalBlocks++;
        }
        this.totalRecords++;
        this.saveLists();
        this.saveHeader();
        return insertResult;
    }

    public BlockInsertResult insertRecordAsNewBlock(T record, int nextBlock, int prevBlock) {
        int blockIndex;
        // Použiť iba emptyBlocks (bloky úplne prázdne) alebo nový blok na konci
        if (!this.emptyBlocks.isEmpty()) {
            blockIndex = this.emptyBlocks.removeFirst();
        } else {
            blockIndex = this.totalBlocks;
        }

        Block<T> block;
        if (blockIndex < this.totalBlocks) {
            // bezpečné: ide o block z emptyBlocks, teda prázdny
            block = this.getBlock(blockIndex);
        } else {
            block = new Block<>(this.recordClass, this.blockSize);
        }

        block.addRecord(record);

        // Aktualizovať zoznamy rovnakým spôsobom ako updateListsAfterInsert
        this.updateListsAfterInsert(blockIndex, block);

        // Nastaviť ukazovatele a zapísať
        block.setNextBlockIndex(nextBlock);
        block.setPreviousBlockIndex(prevBlock);
        this.writeBlockToFile(block, blockIndex);

        if (blockIndex == this.totalBlocks) {
            this.totalBlocks++;
        }
        this.totalRecords++;

        this.saveLists();
        this.saveHeader();

        return new BlockInsertResult(blockIndex, block);
    }

    public void updateChainPointers(int blockIndex, int nextBlock, int prevBlock) {
        Block<T> block = this.getBlock(blockIndex);
        block.setNextBlockIndex(nextBlock);
        block.setPreviousBlockIndex(prevBlock);
        this.writeBlockToFile(block, blockIndex);
    }

    private void applyDeleteMetadata(int blockIndex, Block<T> block) {
        this.totalRecords--;
        this.updateListsAfterDelete(blockIndex, block);
        this.writeBlockToFile(block, blockIndex);
        this.saveLists();
        this.saveHeader();
    }


    public T findRecord(int index, T record) {
        if (index < 0 || index >= this.totalBlocks) {
            return null;
        }

        Block<T> block = this.getBlock(index);
        return block.getCopyOfRecord(record);
    }

    public T findInChain(int startBlockIndex, T recordTemplate) {
        int currentIndex = startBlockIndex;

        while (currentIndex != -1) {
            Block<T> block = this.getBlock(currentIndex);
            T found = block.getCopyOfRecord(recordTemplate);
            if (found != null) {
                return found;
            }
            currentIndex = block.getNextBlockIndex();
        }
        return null;
    }

    public boolean deleteFromChain(int startBlockIndex, T record) {
        int currentIndex = startBlockIndex;

        while (currentIndex != -1) {
            Block<T> block = this.getBlock(currentIndex);
            T removed = block.removeRecord(record);

            if (removed != null) {
                this.applyDeleteMetadata(currentIndex, block);
                return true;
            }

            currentIndex = block.getNextBlockIndex();
        }

        return false;
    }

    private void updateListsAfterInsert(int index, Block<T> block) {
        if (block.getValidCount() == block.getBlockFactor()) {
            this.partiallyEmptyBlocks.remove(Integer.valueOf(index));
        } else if (block.getValidCount() > 0 && block.getValidCount() < block.getBlockFactor()) {
            if (!this.partiallyEmptyBlocks.contains(index)) {
                this.partiallyEmptyBlocks.add(index);
            }
            this.emptyBlocks.remove(Integer.valueOf(index));
        } else if (block.getValidCount() == 0) {
            if (!this.emptyBlocks.contains(index)) {
                this.emptyBlocks.add(index);
            }
            this.partiallyEmptyBlocks.remove(Integer.valueOf(index));
        }
        if (this.blockValidCounts.size() > index) {
            this.blockValidCounts.set(index, block.getValidCount());
        } else {
            this.blockValidCounts.add(block.getValidCount());
        }
    }

    private void updateListsAfterDelete(int index, Block<T> block) {
        if (block.getValidCount() == 0) {
            this.emptyBlocks.add(index);
            this.partiallyEmptyBlocks.remove(Integer.valueOf(index));
        } else if (block.getValidCount() < block.getBlockFactor()) {
            if (!this.partiallyEmptyBlocks.contains(index)) {
                this.partiallyEmptyBlocks.add(index);
            }
        }
        this.blockValidCounts.set(index, block.getValidCount());
    }

    public void trimTrailingEmptyBlocks() {
        int last = this.totalBlocks - 1;

        while (last >= 0 && this.emptyBlocks.contains(last)) {
            last--;
        }

        int numberOfBlocks = this.totalBlocks - (last + 1);
        if (numberOfBlocks <= 0) {
            return;
        }
        this.truncateLastBlock(numberOfBlocks);

        for (int i = 0; i < numberOfBlocks; i++) {
            this.totalBlocks--;
            this.emptyBlocks.remove(Integer.valueOf(this.totalBlocks));
        }
        this.saveHeader();
    }

    public void writeBlockToFile(Block<T> block, int blockIndex) {
        byte[] blockData = block.toByteArray();
        try (RandomAccessFile raf = new RandomAccessFile(this.dataFile, "rw")) {
            raf.seek((long) blockIndex * this.blockSize);
            raf.write(blockData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Block<T> getBlock(int blockIndex) {
        Block<T> block = new Block<>(this.recordClass, this.blockSize);
        try (RandomAccessFile raf = new RandomAccessFile(this.dataFile, "r")) {
            raf.seek((long) blockIndex * this.blockSize);
            byte[] bytes = new byte[this.blockSize];
            raf.readFully(bytes);
            block.fromByteArray(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return block;
    }

    public int allocateEmptyBlock() {
        int index = this.totalBlocks;
        Block<T> block = new Block<>(this.recordClass, this.blockSize);
        this.writeBlockToFile(block, index);
        this.totalBlocks++;
        this.emptyBlocks.add(index);
        this.saveHeader();
        this.saveLists();
        return index;
    }

    public boolean hasFreeSpace(int blockIndex) {
        Block<T> block = this.getBlock(blockIndex);
        return block.getValidCount() < block.getBlockFactor();
    }

    public int getNextBlockIndex(int blockIndex) {
        Block<T> block = this.getBlock(blockIndex);
        return block.getNextBlockIndex();
    }

    public int getPreviousBlockIndex(int blockIndex) {
        Block<T> block = this.getBlock(blockIndex);
        return block.getPreviousBlockIndex();
    }

    private void truncateLastBlock(int numberOfBlocks) {
        try (RandomAccessFile raf = new RandomAccessFile(this.dataFile, "rw")) {
            long newLength = Math.max(0, raf.length() - ((long) this.blockSize * numberOfBlocks));
            raf.setLength(newLength);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveHeader() {
        try (PrintWriter pw = new PrintWriter(new FileWriter(this.headerFile))) {

            pw.println(this.totalBlocks);
            pw.println(this.totalRecords);

            for (int i = 0; i < this.totalBlocks; i++) {
                pw.println(i);                       // index bloku
                pw.println(this.blockValidCounts.get(i)); // počet valid záznamov
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void loadHeader() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.headerFile))) {

            this.totalBlocks = Integer.parseInt(br.readLine());
            this.totalRecords = Integer.parseInt(br.readLine());

            this.blockValidCounts.clear();

            for (int i = 0; i < this.totalBlocks; i++) {
                br.readLine(); // index ignorujeme, aj tak je to i
                int count = Integer.parseInt(br.readLine());
                this.blockValidCounts.add(count);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void saveLists() {
        this.saveListToFile(this.emptyBlocksFile, this.emptyBlocks);
        this.saveListToFile(this.partialBlocksFile, this.partiallyEmptyBlocks);
    }

    private void loadLists() {
        this.loadListFromFile(this.emptyBlocksFile, this.emptyBlocks);
        this.loadListFromFile(this.partialBlocksFile, this.partiallyEmptyBlocks);
    }

    private void saveListToFile(File file, List<Integer> list) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            for (int i : list) {
                pw.println(i);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error saving list file: " + file.getName(), e);
        }
    }

    private void loadListFromFile(File file, List<Integer> list) {
        list.clear();
        if (!file.exists()) return;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    list.add(Integer.parseInt(line.trim()));
                } catch (NumberFormatException ignored) {}
            }
        } catch (IOException e) {
            throw new RuntimeException("Error loading list file: " + file.getName(), e);
        }
    }

    public int getTotalBlocks() { return this.totalBlocks; }
    public int getTotalRecords() { return this.totalRecords; }
    public List<Integer> getEmptyBlocks() { return Collections.unmodifiableList(this.emptyBlocks); }
    public List<Integer> getPartiallyEmptyBlocks() { return Collections.unmodifiableList(this.partiallyEmptyBlocks); }

    public int getBlockSize() {
        return this.blockSize;
    }

    public Class<T> getRecordClass() {
        return this.recordClass;
    }

    public static class BlockInsertResult<T extends IRecord<T>> {
        public final int blockIndex;
        public final Block<T> block;

        public BlockInsertResult(int blockIndex, Block<T> block) {
            this.blockIndex = blockIndex;
            this.block = block;
        }
    }
}

