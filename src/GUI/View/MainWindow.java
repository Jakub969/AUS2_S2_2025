package GUI.View;

import javax.swing.*;
import java.awt.*;
import java.util.Date;

import DS.LinearHashFile;
import DS.Block;
import Interface.IRecord;
import Interface.IHashable;
import Tester.Osoba;

public class MainWindow<T extends IRecord<T> & IHashable> extends JFrame {

    private final LinearHashFile<T> file;

    private JTextArea outputArea;
    private JTextField insertField;
    private JTextField findField;
    private JTextField deleteField;

    public MainWindow(LinearHashFile<T> file) {
        this.file = file;
        this.initUI();
    }

    private void initUI() {
        this.setTitle("LinearHashFile GUI");
        this.setSize(700, 500);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setLayout(new BorderLayout());

        this.outputArea = new JTextArea();
        this.outputArea.setEditable(false);
        this.add(new JScrollPane(this.outputArea), BorderLayout.CENTER);

        JPanel controlPanel = new JPanel(new GridLayout(3, 1));

        // ---- INSERT ----
        JPanel insertPanel = new JPanel(new FlowLayout());
        this.insertField = new JTextField(20);
        JButton insertBtn = new JButton("Insert");
        insertBtn.addActionListener(e -> this.insertRecord());
        insertPanel.add(new JLabel("Insert record:"));
        insertPanel.add(this.insertField);
        insertPanel.add(insertBtn);

        // ---- FIND ----
        JPanel findPanel = new JPanel(new FlowLayout());
        this.findField = new JTextField(20);
        JButton findBtn = new JButton("Find");
        findBtn.addActionListener(e -> this.findRecord());
        findPanel.add(new JLabel("Find key:"));
        findPanel.add(this.findField);
        findPanel.add(findBtn);

        // ---- DELETE ----
        JPanel deletePanel = new JPanel(new FlowLayout());
        this.deleteField = new JTextField(20);
        JButton deleteBtn = new JButton("Delete");
        deleteBtn.addActionListener(e -> this.deleteRecord());
        deletePanel.add(new JLabel("Delete key:"));
        deletePanel.add(this.deleteField);
        deletePanel.add(deleteBtn);

        controlPanel.add(insertPanel);
        controlPanel.add(findPanel);
        controlPanel.add(deletePanel);

        this.add(controlPanel, BorderLayout.NORTH);

        JPanel bottomPanel = new JPanel();
        JButton showBtn = new JButton("Show Buckets");
        showBtn.addActionListener(e -> this.showBuckets());
        bottomPanel.add(showBtn);

        this.add(bottomPanel, BorderLayout.SOUTH);
    }

    // =============================
    // INSERT
    // =============================
    private void insertRecord() {
        String val = this.insertField.getText().trim();
        if (val.isEmpty()) {
            this.outputArea.setText("Empty input.");
            return;
        }

        try {
            // Tu musíš vytvoriť objekt svojho typu T
            // Napríklad: T r = (T) new Osoba(val); alebo new MyRecord(Integer.parseInt(val));
            // Toto si doplň podľa skutočného record typu
            T record = this.createRecordFromString(val);

            this.file.insert(record);
            this.outputArea.setText("Record inserted.\n\n");
            this.showBuckets();
        } catch (Exception ex) {
            this.outputArea.setText("Insert failed: " + ex.getMessage());
        }
    }

    // =============================
    // FIND
    // =============================
    private void findRecord() {
        String key = this.findField.getText().trim();
        if (key.isEmpty()) {
            this.outputArea.setText("Empty key.");
            return;
        }

        try {
            T probe = this.createRecordFromString(key);

            T found = this.file.find(probe);
            if (found == null) {
                this.outputArea.setText("Record not found.");
            } else {
                this.outputArea.setText("Found:\n" + found.toString());
            }
        } catch (Exception e) {
            this.outputArea.setText("Find failed: " + e.getMessage());
        }
    }

    // =============================
    // DELETE
    // =============================
    private void deleteRecord() {
        String key = this.deleteField.getText().trim();
        if (key.isEmpty()) {
            this.outputArea.setText("Empty key.");
            return;
        }

        try {
            T probe = this.createRecordFromString(key);

            boolean deleted = this.file.delete(probe);
            if (deleted) {
                this.outputArea.setText("Record deleted.\n\n");
                this.showBuckets();
            } else {
                this.outputArea.setText("Record not found.");
            }
        } catch (Exception e) {
            this.outputArea.setText("Delete failed: " + e.getMessage());
        }
    }

    // =============================
    // BUCKET VIEW
    // =============================
    private void showBuckets() {
        this.outputArea.append("=== BUCKETS ===\n");

        int n = this.file.getNumberOfBuckets();
        for (int i = 0; i < n; i++) {
            this.outputArea.append("Bucket " + i + ":\n");

            int headIdx = this.file.getBucketPointer(i);
            if (headIdx == -1) {
                this.outputArea.append("  Empty\n");
                continue;
            }

            int cur = headIdx;

            Block<T> b = this.file.getPrimaryFile().getBlock(cur);
            this.outputArea.append("  Block " + cur + " (" + b.getValidCount() + " records):\n");
            for (int j = 0; j < b.getValidCount(); j++) {
                this.outputArea.append("    " + b.getRecordAt(j) + "\n");
            }
            cur = b.getNextBlockIndex();
            while (cur != -1) {
                b = this.file.getOverflowFile().getBlock(cur);
                this.outputArea.append("  Block " + cur + " (" + b.getValidCount() + " records):\n");
                for (int j = 0; j < b.getValidCount(); j++) {
                    this.outputArea.append("    " + b.getRecordAt(j) + "\n");
                }
                cur = b.getNextBlockIndex();
            }

            this.outputArea.append("\n");
        }
    }

    private T createRecordFromString(String s) {
        try {
            // VLOŽENIE: meno;priezvisko;dd.MM.yyyy;UUID
            if (s.contains(";")) {
                String[] parts = s.split(";");
                if (parts.length != 4) {
                    throw new IllegalArgumentException("Format: meno;priezvisko;dd.MM.yyyy;UUID");
                }

                String meno = parts[0];
                String priez = parts[1];

                java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("dd.MM.yyyy");
                Date date = df.parse(parts[2]);

                String uuid = parts[3];

                return (T) new Osoba(meno, priez, date, uuid);
            }

            // FIND/DELETE: iba UUID
            return (T) Osoba.fromUUID(s);

        } catch (Exception ex) {
            throw new RuntimeException("Invalid record input: " + ex.getMessage());
        }
    }
}
