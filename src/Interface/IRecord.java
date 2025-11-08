package Interface;

public interface IRecord<T> extends IByteOperation<T> {
    boolean isEqual(T object);
    T createCopy();

    @Override
    T fromByteArray(byte[] bytesArray);

    @Override
    byte[] toByteArray();

    @Override
    int getSize();
}
