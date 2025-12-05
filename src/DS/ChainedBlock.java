package DS;

import Interface.IHashable;
import Interface.IRecord;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

public class ChainedBlock<T extends IRecord<T>> extends Block<T> {
    private int nextBlockIndex;

    public ChainedBlock(Class<T> recordType, int sizeOfBlock) {
        super(recordType, sizeOfBlock);
        this.nextBlockIndex = -1;
        int actualSizeForRecords = this.blockSize - (2 * Integer.BYTES);
        this.blockFactor = actualSizeForRecords / this.recordSize;
        this.records = new IRecord[this.blockFactor];
    }

    @Override
    public T fromByteArray(byte[] bytesArray) {
        this.clearBlock();
        ByteArrayInputStream hlpByteArrayInputStream = new ByteArrayInputStream(bytesArray);
        DataInputStream hlpInStream = new DataInputStream(hlpByteArrayInputStream);
        try {
            this.validCount = hlpInStream.readInt();
            this.nextBlockIndex = hlpInStream.readInt();
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

    @Override
    protected void clearBlock() {
        super.clearBlock();
        this.nextBlockIndex = -1;
    }

    @Override
    public byte[] toByteArray() {
        ByteArrayOutputStream hlpByteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream hlpOutStream = new DataOutputStream(hlpByteArrayOutputStream);
        try {
            hlpOutStream.writeInt(this.validCount);
            hlpOutStream.writeInt(this.nextBlockIndex);
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

    public int getNextBlockIndex() {
        return this.nextBlockIndex;
    }

    public void setNextBlockIndex(int nextBlockIndex) {
        this.nextBlockIndex = nextBlockIndex;
    }

    public void setValidCount(int validCount) {
        this.validCount = validCount;
    }

    @Override
    public int getBlockFactor() {
        return this.blockFactor;
    }

    public void updateRecordAt(int r, T newRecord) {
        this.records[r] = newRecord;
    }

    public boolean updateRecord(T newRecord) {
        for (int i = 0; i < this.blockFactor; i++) {
            T currentRecord = (T) this.records[i];
            if (currentRecord != null && currentRecord.isEqual(newRecord)) {
                this.records[i] = newRecord;
                return true;
            }
        }
        return false;
    }
}
