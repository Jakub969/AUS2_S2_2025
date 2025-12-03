package DS;

import Interface.IByteOperation;
import Interface.IRecord;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class Block<T extends IRecord<T>> implements IByteOperation<T> {
    protected int validCount;
    protected IRecord<T>[] records;
    protected int blockFactor;
    protected final Class<T> recordType;
    protected int recordSize;
    protected final int blockSize;


    public Block(Class<T> recordType, int sizeOfBlock) {
        this.recordType = recordType;
        this.recordSize = this.getSizeOfRecord();
        this.blockSize = sizeOfBlock;
        int actualSizeOfBlock = this.blockSize - Integer.BYTES;
        this.blockFactor = actualSizeOfBlock / this.recordSize;
        this.records = new IRecord[this.blockFactor];
        this.validCount = 0;
    }

    private int getSizeOfRecord() {
        try {
            return this.recordType.getDeclaredConstructor().newInstance().getSize();
        } catch (InstantiationException e) {
            throw new IllegalStateException("Cannot instantiate record type", e);
        } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T fromByteArray(byte[] bytesArray) {
        this.clearBlock();
        ByteArrayInputStream hlpByteArrayInputStream = new ByteArrayInputStream(bytesArray);
        DataInputStream hlpInStream = new DataInputStream(hlpByteArrayInputStream);
        try {
            this.validCount = hlpInStream.readInt();
            for (int i = 0; i < this.blockFactor; i++) {
                byte[] recordBytes = new byte[this.recordSize];
                hlpInStream.read(recordBytes);
                T recordInstance = this.recordType.getDeclaredConstructor().newInstance();
                IRecord<T> record = recordInstance.fromByteArray(recordBytes);
                this.records[i] = record;
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot deserialize record", e);
        } catch (InstantiationException e) {
            throw new IllegalStateException("Cannot instantiate record type", e);
        } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    protected void clearBlock() {
        Arrays.fill(this.records, null);
        this.validCount = 0;
    }

    @Override
    public byte[] toByteArray() {
        ByteArrayOutputStream hlpByteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream hlpOutStream = new DataOutputStream(hlpByteArrayOutputStream);
        try {
            hlpOutStream.writeInt(this.validCount);
            for (int i = 0; i < this.blockFactor; i++) {
                if (this.records[i] != null) {
                    hlpOutStream.write(this.records[i].toByteArray());
                } else {
                    hlpOutStream.write(new byte[this.recordSize]);
                }
            }
            int currentSize = hlpByteArrayOutputStream.size();
            if (currentSize < this.blockSize) {
                hlpOutStream.write(new byte[this.blockSize - currentSize]);
            }
            return hlpByteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize record", e);
        }
    }

    @Override
    public int getSize() {
        return this.blockSize;
    }

    public IRecord<T> getRecordAt(int index) {
        return this.records[index];
    }


    public T getCopyOfRecord(T record) {
        for (int i = 0; i < this.blockFactor; i++) {
            IRecord<T> currentRecord = this.records[i];
            if (currentRecord != null && currentRecord.isEqual(record)) {
                return currentRecord.createCopy();
            }
        }
        return null;
    }

    public void addRecord(T record) {
        if (this.validCount >= this.blockFactor) {
            return;
        }
        this.records[this.validCount] = record;
        this.validCount++;
    }


    public T removeRecord(T record) {
        for (int i = 0; i < this.blockFactor; i++) {
            IRecord<T> currentRecord = this.records[i];
            if (currentRecord != null && currentRecord.isEqual(record)) {
                T copy = currentRecord.createCopy();
                this.validCount--;
                this.compact(i, copy);
                return copy;
            }
        }
        return null;
    }

    private void compact(int removedIndex, T recordCopy) {
        for (int i = removedIndex; i < this.validCount; i++) {
            this.records[i] = this.records[i + 1];
        }
        this.records[this.validCount] = recordCopy;
    }

    public void printRecords() {
        for (int i = 0; i < this.blockFactor; i++) {
            IRecord<T> currentRecord = this.records[i];
            if (currentRecord != null) {
                System.out.println(currentRecord);
            }
        }
    }

    public int getValidCount() {
        return this.validCount;
    }

    public int getBlockFactor() {
        return this.blockFactor;
    }
}
