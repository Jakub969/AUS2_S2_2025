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
        if (this.dataFile.exists()) {
            this.loadLists();
            this.loadHeader();
        } else {
            this.saveHeader();
            this.saveLists();
        }
    }

    public int insertRecord(T record) {
        int blockIndex;

        if (!this.partiallyEmptyBlocks.isEmpty()) {
            blockIndex = this.partiallyEmptyBlocks.removeFirst();
        } else if (!this.emptyBlocks.isEmpty()) {
            blockIndex = this.emptyBlocks.removeFirst();
        } else {
            blockIndex = this.totalBlocks;
        }

        Block<T> block;
        if (blockIndex < this.totalBlocks) {
            block = this.getBlock(blockIndex);
        } else {
            block = new Block<>(this.recordClass, this.blockSize);
        }

        block.addRecord(record);
        this.updateListsAfterInsert(blockIndex, block);
        this.writeBlockToFile(block, blockIndex);
        if (blockIndex == this.totalBlocks) {
            this.totalBlocks++;
        }
        this.totalRecords++;

        this.saveLists();
        this.saveHeader();
        return blockIndex;
    }

    public BlockInsertResult insertRecordWithMetadata(T record, int nextBlock, int prevBlock) {
        int blockIndex = this.insertRecord(record);
        Block<T> block = this.getBlock(blockIndex);
        block.setNextBlockIndex(nextBlock);
        block.setPreviousBlockIndex(prevBlock);
        this.writeBlockToFile(block, blockIndex);
        return new BlockInsertResult(blockIndex, block);
    }

    public void updateChainPointers(int blockIndex, int nextBlock, int prevBlock) {
        Block<T> block = this.getBlock(blockIndex);
        block.setNextBlockIndex(nextBlock);
        block.setPreviousBlockIndex(prevBlock);
        this.writeBlockToFile(block, blockIndex);
    }

    public boolean deleteRecord(int index, T record) {
        if (index < 0 || index >= this.totalBlocks) {
            return false;
        }

        Block<T> block = this.getBlock(index);
        T removed = block.removeRecord(record);

        if (removed == null) {
            return false;
        }

        this.totalRecords--;
        this.updateListsAfterDelete(index, block);
        this.writeBlockToFile(block, index);
        this.trimTrailingEmptyBlocks();

        this.saveLists();
        this.saveHeader();
        return true;
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
                this.writeBlockToFile(block, currentIndex);
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
            if (!this.partiallyEmptyBlocks.contains(index))
                this.partiallyEmptyBlocks.add(index);
            this.emptyBlocks.remove(Integer.valueOf(index));
        } else if (block.getValidCount() == 0) {
            if (!this.emptyBlocks.contains(index))
                this.emptyBlocks.add(index);
            this.partiallyEmptyBlocks.remove(Integer.valueOf(index));
        }
    }

    private void updateListsAfterDelete(int index, Block<T> block) {
        if (block.getValidCount() == 0) {
            this.emptyBlocks.add(index);
            this.partiallyEmptyBlocks.remove(Integer.valueOf(index));
        } else if (block.getValidCount() < block.getBlockFactor()) {
            if (!this.partiallyEmptyBlocks.contains(index))
                this.partiallyEmptyBlocks.add(index);
        }
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadHeader() {
        if (!this.headerFile.exists()) return;
        try (BufferedReader br = new BufferedReader(new FileReader(this.headerFile))) {
            this.totalBlocks = Integer.parseInt(br.readLine());
            this.totalRecords = Integer.parseInt(br.readLine());
        } catch (IOException | NumberFormatException e) {
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

