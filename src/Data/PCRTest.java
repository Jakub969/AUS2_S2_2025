package Data;

import Interface.IHashable;
import Interface.IRecord;

import java.io.*;
import java.util.Date;

public class PCRTest implements IRecord<PCRTest>, IHashable {
    private Date datumTestu;
    private String UUIDPacienta;
    private final int MAX_UUID_LENGTH = 10;
    private int kodTestu;
    private boolean vysledokTestu;
    private double hodnotaTestu;
    private String poznamka;
    private final int MAX_POZNAMKA_LENGTH = 11;

    public PCRTest() {
        this.datumTestu = new Date(0);
        this.UUIDPacienta = "";
        this.kodTestu = 0;
        this.vysledokTestu = false;
        this.hodnotaTestu = 0.0;
        this.poznamka = "";
    }

    public int getKodTestu() {
        return this.kodTestu;
    }

    public boolean isVysledokTestu() {
        return this.vysledokTestu;
    }

    public double getHodnotaTestu() {
        return this.hodnotaTestu;
    }

    public String getPoznamka() {
        return this.poznamka;
    }

    public String getUUIDPacienta() {
        return this.UUIDPacienta;
    }

    public Date getDatumTestu() {
        return this.datumTestu;
    }

    public PCRTest(Date datumTestu, String UUIDPacienta, int kodTestu, boolean vysledokTestu, double hodnotaTestu, String poznamka) {
        this.datumTestu = datumTestu;
        this.UUIDPacienta = UUIDPacienta;
        this.kodTestu = kodTestu;
        this.vysledokTestu = vysledokTestu;
        this.hodnotaTestu = hodnotaTestu;
        this.poznamka = poznamka;
    }

    public static PCRTest fromTestID(int kodTestu) {
        return new PCRTest(new Date(0), "", kodTestu, false, 0.0, "");
    }

    @Override
    public long getHash() {
        return this.kodTestu;
    }

    @Override
    public boolean isEqual(PCRTest object) {
        return this.kodTestu == object.kodTestu;
    }

    @Override
    public PCRTest createCopy() {
        return new PCRTest(this.datumTestu, this.UUIDPacienta, this.kodTestu, this.vysledokTestu, this.hodnotaTestu, this.poznamka);
    }

    @Override
    public PCRTest fromByteArray(byte[] bytesArray) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytesArray))) {
            this.datumTestu = new Date(in.readLong());
            int uuidLength = in.readInt();
            this.UUIDPacienta = this.readFixedString(in, this.MAX_UUID_LENGTH).substring(0, uuidLength);
            this.kodTestu = in.readInt();
            this.vysledokTestu = in.readBoolean();
            this.hodnotaTestu = in.readDouble();
            int poznamkaLength = in.readInt();
            this.poznamka = this.readFixedString(in, this.MAX_POZNAMKA_LENGTH).substring(0, poznamkaLength);
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toByteArray() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(this.datumTestu.getTime());
            out.writeInt(Math.min(this.UUIDPacienta.length(), this.MAX_UUID_LENGTH));
            this.writeFixedString(out, this.UUIDPacienta, this.MAX_UUID_LENGTH);
            out.writeInt(this.kodTestu);
            out.writeBoolean(this.vysledokTestu);
            out.writeDouble(this.hodnotaTestu);
            out.writeInt(Math.min(this.poznamka.length(), this.MAX_POZNAMKA_LENGTH));
            this.writeFixedString(out, this.poznamka, this.MAX_POZNAMKA_LENGTH);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeFixedString(DataOutputStream out, String value, int maxLen) throws IOException {
        for (int i = 0; i < maxLen; i++) {
            char c = (i < value.length()) ? value.charAt(i) : 0;
            out.writeChar(c);
        }
    }

    private String readFixedString(DataInputStream in, int maxLen) throws IOException {
        char[] chars = new char[maxLen];
        for (int i = 0; i < maxLen; i++) {
            chars[i] = in.readChar();
        }
        return new String(chars).replace("\u0000", "");
    }

    @Override
    public String toString() {
        return "PCRTest{" +
                "datumTestu=" + this.datumTestu +
                ", UUIDPacienta='" + this.UUIDPacienta + '\'' +
                ", kodTestu=" + this.kodTestu +
                ", vysledokTestu=" + this.vysledokTestu +
                ", hodnotaTestu=" + this.hodnotaTestu +
                ", poznamka='" + this.poznamka + '\'' +
                '}';
    }

    @Override
    public int getSize() {
        return Long.BYTES +
                Integer.BYTES * 3 +
                1 +
                Double.BYTES +
                (this.MAX_UUID_LENGTH + this.MAX_POZNAMKA_LENGTH) * Character.BYTES;
    }
}
