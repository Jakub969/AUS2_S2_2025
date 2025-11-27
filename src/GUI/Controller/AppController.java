package GUI.Controller;

import DS.HeapFile;
import DS.Block;
import DS.LinearHashFile;
import Tester.Osoba;

import java.util.ArrayList;
import java.util.List;

public class AppController {

    private final LinearHashFile<Osoba> hashFile;

    public AppController(LinearHashFile<Osoba> hashFile) {
        this.hashFile = hashFile;
    }

    public void insertOsoba(Osoba osoba) {
        this.hashFile.insert(osoba);
    }

    public void deleteOsoba(Osoba osoba) {
        this.hashFile.deleteByKey(osoba);
    }

    public Osoba findOsoba(Osoba osoba) {
        return this.hashFile.findByKey(osoba);
    }

    public List<Block<Osoba>> loadBlocks() {
        int total = this.hashFile.getNumberOfBuckets();
        List<Block<Osoba>> list = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            Block<Osoba> block = this.hashFile.getBucket(i);
            list.add(block);
        }
        return list;
    }
}