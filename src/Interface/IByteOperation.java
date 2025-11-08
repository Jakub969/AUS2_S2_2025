package Interface;

public interface IByteOperation<T> {
    T fromByteArray(byte[] bytesArray);
    byte[] toByteArray();
    int getSize();
}
