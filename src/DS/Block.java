package DS;

import Interface.IByteOperation;
import Interface.IRecord;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class Block<T extends IRecord<T>> implements IByteOperation<T> {
    private int validCount;
    private final IRecord<T>[] records;
    private final int blockFactor;
    private int nextBlockIndex;
    private int previousBlockIndex;
    private final Class<T> recordType;
    private final int recordSize;

    public Block(Class<T> recordType, int sizeOfBlock) {
        this.recordType = recordType;
        this.recordSize = this.getSizeOfRecord();
        int actualSizeOfBlock = sizeOfBlock - (3 * Integer.BYTES);
        this.blockFactor = actualSizeOfBlock / this.recordSize;
        this.records = new IRecord[this.blockFactor];
        this.nextBlockIndex = -1;
        this.previousBlockIndex = -1;
        this.validCount = 0;
    }

    private int getSizeOfRecord() {
        try {
            T instance = this.recordType.getDeclaredConstructor().newInstance();
            return instance.getSize();
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
            this.nextBlockIndex = hlpInStream.readInt();
            this.previousBlockIndex = hlpInStream.readInt();
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

    private void clearBlock() {
        Arrays.fill(this.records, null);
        this.validCount = 0;
        this.nextBlockIndex = -1;
        this.previousBlockIndex = -1;
    }

    @Override
    public byte[] toByteArray() {
        ByteArrayOutputStream hlpByteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream hlpOutStream = new DataOutputStream(hlpByteArrayOutputStream);
        try {
            hlpOutStream.writeInt(this.validCount);
            hlpOutStream.writeInt(this.nextBlockIndex);
            hlpOutStream.writeInt(this.previousBlockIndex);
            for (int i = 0; i < this.blockFactor; i++) {
                if (this.records[i] != null) {
                    hlpOutStream.write(this.records[i].toByteArray());
                } else {
                    hlpOutStream.write(new byte[this.recordSize]);
                }
            }
            int currentSize = hlpByteArrayOutputStream.size();
            int totalSize = this.getSize();
            if (currentSize < totalSize) {
                hlpOutStream.write(new byte[totalSize - currentSize]);
            }
            return hlpByteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize record", e);
        }
    }

    @Override
    public int getSize() {
        return 3 * Integer.BYTES + (this.recordSize * this.blockFactor);
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

    public boolean addRecord(T record) {
        if (this.validCount >= this.blockFactor) {
            return false;
        }
        for (int i = 0; i < this.blockFactor; i++) {
            if (this.records[i] == null) {
                this.records[i] = record;
                this.validCount++;
                return true;
            }
        }
        return false;
    }

    public T removeRecord(T record) {
        for (int i = 0; i < this.blockFactor; i++) {
            IRecord<T> currentRecord = this.records[i];
            if (currentRecord != null && currentRecord.isEqual(record)) {
                this.records[i] = null;
                this.validCount--;
                return currentRecord.createCopy();
            }
        }
        return null;
    }

    public void printRecords() {
        for (int i = 0; i < this.blockFactor; i++) {
            IRecord<T> currentRecord = this.records[i];
            if (currentRecord != null) {
                System.out.println(currentRecord.toString());
            }
        }
    }

    public void setNextBlockIndex(int nextBlockIndex) {
        this.nextBlockIndex = nextBlockIndex;
    }

    public void setPreviousBlockIndex(int previousBlockIndex) {
        this.previousBlockIndex = previousBlockIndex;
    }

    public int getNextBlockIndex() {
        return nextBlockIndex;
    }

    public int getPreviousBlockIndex() {
        return previousBlockIndex;
    }

    public int getValidCount() {
        return validCount;
    }
}
