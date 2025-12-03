package DS;

import Interface.IRecord;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

public class ChainedBlock extends Block {
    private int nextBlockIndex;

    public ChainedBlock(Class recordType, int sizeOfBlock) {
        super(recordType, sizeOfBlock);
        int actualSizeOfBlock = sizeOfBlock - (2*Integer.BYTES);
        this.blockFactor = actualSizeOfBlock / this.recordSize;
        this.nextBlockIndex = -1;
    }

    @Override
    public IRecord fromByteArray(byte[] bytesArray) {
        this.clearBlock();
        ByteArrayInputStream hlpByteArrayInputStream = new ByteArrayInputStream(bytesArray);
        DataInputStream hlpInStream = new DataInputStream(hlpByteArrayInputStream);
        try {
            this.validCount = hlpInStream.readInt();
            this.nextBlockIndex = hlpInStream.readInt();
            for (int i = 0; i < this.blockFactor; i++) {
                byte[] recordBytes = new byte[this.recordSize];
                hlpInStream.read(recordBytes);
                IRecord recordInstance = (IRecord) this.recordType.getDeclaredConstructor().newInstance();
                IRecord record = (IRecord) recordInstance.fromByteArray(recordBytes);
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
}
