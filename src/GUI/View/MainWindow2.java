package GUI.View;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import GUI.Controller.Controller;

public class MainWindow2 extends JFrame {

    private final Controller controller;
    private JTextArea outputArea;
    private JLabel osobaStatusLabel;
    private JLabel pcrStatusLabel;

    public MainWindow2() {
        this.controller = new Controller();
        this.initUI();
        this.updateStatusLabels();
    }

    private void initUI() {
        this.setTitle("Hash File Management System");
        this.setSize(900, 700);
        this.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);

        this.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                MainWindow2.this.closeAllFiles();
                System.exit(0);
            }
        });

        this.setLayout(new BorderLayout());

        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("File");

        JMenuItem newModelItem = new JMenuItem("New Model...");
        newModelItem.addActionListener(e -> this.createNewModel());

        JMenuItem openModelItem = new JMenuItem("Open Model...");
        openModelItem.addActionListener(e -> this.openModel());

        JMenuItem generateDataItem = new JMenuItem("Generate Test Data");
        generateDataItem.addActionListener(e -> this.generateTestData());

        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.addActionListener(e -> {
            this.closeAllFiles();
            System.exit(0);
        });

        fileMenu.add(newModelItem);
        fileMenu.add(openModelItem);
        fileMenu.addSeparator();
        fileMenu.add(generateDataItem);
        fileMenu.addSeparator();
        fileMenu.add(exitItem);

        menuBar.add(fileMenu);
        this.setJMenuBar(menuBar);

        JTabbedPane tabbedPane = new JTabbedPane();

        JPanel osobaPanel = this.createOsobaPanel();
        tabbedPane.addTab("Osoba Records", osobaPanel);

        JPanel pcrPanel = this.createPCRPanel();
        tabbedPane.addTab("PCR Test Records", pcrPanel);

        this.outputArea = new JTextArea();
        this.outputArea.setEditable(false);
        JScrollPane outputScroll = new JScrollPane(this.outputArea);
        outputScroll.setPreferredSize(new Dimension(880, 350));

        JPanel statusPanel = new JPanel(new GridLayout(1, 2));
        this.osobaStatusLabel = new JLabel("Osoba: Not loaded");
        this.pcrStatusLabel = new JLabel("PCR Test: Not loaded");
        this.osobaStatusLabel.setBorder(BorderFactory.createEtchedBorder());
        this.pcrStatusLabel.setBorder(BorderFactory.createEtchedBorder());
        statusPanel.add(this.osobaStatusLabel);
        statusPanel.add(this.pcrStatusLabel);

        this.add(tabbedPane, BorderLayout.CENTER);
        this.add(outputScroll, BorderLayout.SOUTH);
        this.add(statusPanel, BorderLayout.NORTH);
    }

    private JPanel createOsobaPanel() {
        JPanel panel = new JPanel(new BorderLayout());

        JPanel inputPanel = new JPanel(new GridLayout(4, 2, 5, 5));
        inputPanel.setBorder(BorderFactory.createTitledBorder("Osoba Data"));

        JTextField menoField = new JTextField(20);
        JTextField priezviskoField = new JTextField(20);
        JTextField datumField = new JTextField(20);
        JTextField uuidField = new JTextField(20);

        inputPanel.add(new JLabel("Meno:"));
        inputPanel.add(menoField);
        inputPanel.add(new JLabel("Priezvisko:"));
        inputPanel.add(priezviskoField);
        inputPanel.add(new JLabel("Dátum nar. (dd.MM.yyyy):"));
        inputPanel.add(datumField);
        inputPanel.add(new JLabel("UUID:"));
        inputPanel.add(uuidField);

        JPanel buttonPanel = new JPanel(new FlowLayout());
        JButton insertBtn = new JButton("Insert");
        JButton findBtn = new JButton("Find");
        JButton editBtn = new JButton("Edit");
        JButton showBtn = new JButton("Show Buckets");
        JButton showPrimary = new JButton("Show Primary file");
        JButton showoverflow = new JButton("Show overflow file");
        JButton clearBtn = new JButton("Clear");

        insertBtn.addActionListener(e -> {
            this.insertOsoba(menoField.getText(), priezviskoField.getText(),
                    datumField.getText(), uuidField.getText());
            this.updateStatusLabels();
        });

        findBtn.addActionListener(e -> {
            this.findOsoba(uuidField.getText());
        });

        editBtn.addActionListener(e -> {
            this.updateOsoba(menoField.getText(), priezviskoField.getText(),
                    datumField.getText(), uuidField.getText());
            this.updateStatusLabels();
        });

        showBtn.addActionListener(e -> {
            this.showOsobaBuckets();
        });

        showPrimary.addActionListener(e -> {this.showPrimarySequencePrint("Osoba");});
        showoverflow.addActionListener(e -> {this.showOverflowSequencePrint("Osoba");});

        clearBtn.addActionListener(e -> {
            menoField.setText("");
            priezviskoField.setText("");
            datumField.setText("");
            uuidField.setText("");
            this.outputArea.setText("");
        });

        buttonPanel.add(insertBtn);
        buttonPanel.add(findBtn);
        buttonPanel.add(editBtn);
        buttonPanel.add(showBtn);
        buttonPanel.add(showPrimary);
        buttonPanel.add(showoverflow);
        buttonPanel.add(clearBtn);

        panel.add(inputPanel, BorderLayout.NORTH);
        panel.add(buttonPanel, BorderLayout.CENTER);

        return panel;
    }

    private void updateOsoba(String meno, String priezvisko, String datum, String UUID) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded. Please create or open a model first.");
            return;
        }
        try {
            SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy");
            Date date = df.parse(datum);

            boolean success = this.controller.editOsoba(meno, priezvisko, date, UUID);
            if (success) {
                this.outputArea.setText("Osoba updated successfully.\nMeno: " + meno + ", Priezvisko: " + priezvisko + ", Dátum: " + date);
            } else {
                this.outputArea.setText("Failed to update Osoba.");
            }
        } catch (Exception e) {
            this.outputArea.setText("Update failed: " + e.getMessage());
        }
    }

    private JPanel createPCRPanel() {
        JPanel panel = new JPanel(new BorderLayout());

        JPanel inputPanel = new JPanel(new GridLayout(6, 2, 5, 5));
        inputPanel.setBorder(BorderFactory.createTitledBorder("PCR Test Data"));

        JTextField datumField = new JTextField(20);
        JTextField uuidField = new JTextField(20);
        JTextField kodField = new JTextField(20);
        JTextField hodnotaField = new JTextField(20);
        JCheckBox vysledokCheck = new JCheckBox("Positive");
        JTextField poznamkaField = new JTextField(20);

        inputPanel.add(new JLabel("Dátum testu (dd.MM.yyyy):"));
        inputPanel.add(datumField);
        inputPanel.add(new JLabel("UUID pacienta:"));
        inputPanel.add(uuidField);
        inputPanel.add(new JLabel("Kód testu:"));
        inputPanel.add(kodField);
        inputPanel.add(new JLabel("Hodnota testu:"));
        inputPanel.add(hodnotaField);
        inputPanel.add(new JLabel("Výsledok:"));
        inputPanel.add(vysledokCheck);
        inputPanel.add(new JLabel("Poznámka:"));
        inputPanel.add(poznamkaField);

        JPanel buttonPanel = new JPanel(new FlowLayout());
        JButton insertBtn = new JButton("Insert");
        JButton findBtn = new JButton("Find");
        JButton editBtn = new JButton("Edit");
        JButton showBtn = new JButton("Show Buckets");
        JButton showPrimary = new JButton("Show Primary file");
        JButton showoverflow = new JButton("Show overflow file");
        JButton clearBtn = new JButton("Clear");

        insertBtn.addActionListener(e -> {
            this.insertPCRTest(datumField.getText(), uuidField.getText(),
                    kodField.getText(), hodnotaField.getText(),
                    vysledokCheck.isSelected(), poznamkaField.getText());
            this.updateStatusLabels();
        });

        findBtn.addActionListener(e -> {
            this.findPCRTest(kodField.getText());
        });

        editBtn.addActionListener(e -> {
            this.updatePCRTest(datumField.getText(), hodnotaField.getText(),
                    vysledokCheck.isSelected(), poznamkaField.getText(), Integer.parseInt(kodField.getText()));
            this.updateStatusLabels();
        });

        showBtn.addActionListener(e -> {
            this.showPCRBuckets();
        });

        showPrimary.addActionListener(e -> {this.showPrimarySequencePrint("Test");});
        showoverflow.addActionListener(e -> {this.showOverflowSequencePrint("Test");});

        clearBtn.addActionListener(e -> {
            datumField.setText("");
            uuidField.setText("");
            kodField.setText("");
            hodnotaField.setText("");
            vysledokCheck.setSelected(false);
            poznamkaField.setText("");
            this.outputArea.setText("");
        });

        buttonPanel.add(insertBtn);
        buttonPanel.add(findBtn);
        buttonPanel.add(editBtn);
        buttonPanel.add(showBtn);
        buttonPanel.add(showPrimary);
        buttonPanel.add(showoverflow);
        buttonPanel.add(clearBtn);

        panel.add(inputPanel, BorderLayout.NORTH);
        panel.add(buttonPanel, BorderLayout.CENTER);

        return panel;
    }

    private void showOverflowSequencePrint(String className) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }
        try {
            String result = this.controller.getOverflowSequencePrint(className);
            this.outputArea.setText(result);
        } catch (Exception e) {
            this.outputArea.setText("Operation failed: " + e.getMessage());
        }
    }

    private void showPrimarySequencePrint(String className) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }
        try {
            String result = this.controller.getPrimarySequencePrint(className);
            this.outputArea.setText(result);
        } catch (Exception e) {
            this.outputArea.setText("Operation failed: " + e.getMessage());
        }
    }

    private void updatePCRTest(String datum, String hodnota, boolean vysledok, String poznamka, int kodTestu) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded. Please create or open a model first.");
            return;
        }
        try {
            SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy");
            Date date = df.parse(datum);
            double hodnotaTestu = Double.parseDouble(hodnota);

            boolean success = this.controller.editPCRTest(date, hodnotaTestu, vysledok, poznamka, kodTestu);
            if (success) {
                this.outputArea.setText("PCR Test updated successfully.\nDátum: " + date +
                        ", Hodnota: " + hodnotaTestu + ", Výsledok: " + vysledok +
                        ", Poznámka: " + poznamka);
            } else {
                this.outputArea.setText("Failed to update PCR Test.");
            }
        } catch (Exception e) {
            this.outputArea.setText("Update failed: " + e.getMessage());
        }
    }

    private void createNewModel() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Select Folder for New Model");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File folder = fileChooser.getSelectedFile();
            String folderPath = folder.getAbsolutePath();

            String initialBucketsStr = JOptionPane.showInputDialog(this,
                    "Initial number of buckets:", "4");
            String blockSizePrimaryStr = JOptionPane.showInputDialog(this,
                    "Primary block size (bytes):", "512");
            String blockSizeOverflowStr = JOptionPane.showInputDialog(this,
                    "Overflow block size (bytes):", "256");

            try {
                int initialBuckets = Integer.parseInt(initialBucketsStr);
                int blockSizePrimary = Integer.parseInt(blockSizePrimaryStr);
                int blockSizeOverflow = Integer.parseInt(blockSizeOverflowStr);

                this.controller.createNewModelWithParams(
                        folderPath + File.separator + "osoba_data",
                        folderPath + File.separator + "pcr_data",
                        initialBuckets, blockSizePrimary, blockSizeOverflow);

                this.outputArea.setText("New model created in: " + folderPath + "\n");
                this.updateStatusLabels();
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Error creating model: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void openModel() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Open Model Folder");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            File folder = fileChooser.getSelectedFile();
            String folderPath = folder.getAbsolutePath();

            try {
                this.controller.openExistingModel(
                        folderPath + File.separator + "osoba_data",
                        folderPath + File.separator + "pcr_data");

                this.outputArea.setText("Model opened from: " + folderPath + "\n");
                this.updateStatusLabels();
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Error opening model: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void generateTestData() {
        if (!this.controller.isModelLoaded()) {
            JOptionPane.showMessageDialog(this,
                    "Please create or open a model first.",
                    "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        String countStr = JOptionPane.showInputDialog(this,
                "Number of persons to generate:", "10");

        try {
            int count = Integer.parseInt(countStr);
            this.controller.generateData(count);
            this.outputArea.setText("Generated " + count + " persons with random PCR tests.\n");
            this.updateStatusLabels();
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this,
                    "Error generating data: " + e.getMessage(),
                    "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void closeAllFiles() {
        this.controller.closeModel();
        this.updateStatusLabels();
    }

    private void updateStatusLabels() {
        if (this.controller.isOsobaFileLoaded()) {
            this.osobaStatusLabel.setText("Osoba: " + this.controller.getCurrentOsobaFolder());
        } else {
            this.osobaStatusLabel.setText("Osoba: Not loaded");
        }

        if (this.controller.isPCRFileLoaded()) {
            this.pcrStatusLabel.setText("PCR: " + this.controller.getCurrentPCRFolder());
        } else {
            this.pcrStatusLabel.setText("PCR: Not loaded");
        }
    }

    private void insertOsoba(String meno, String priezvisko, String datum, String uuid) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded. Please create or open a model first.");
            return;
        }

        try {
            SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy");
            Date date = df.parse(datum);

            boolean success = this.controller.insertOsoba(meno, priezvisko, date, uuid);
            if (success) {
                this.outputArea.setText("Osoba inserted successfully.\nUUID: " + uuid);
            } else {
                this.outputArea.setText("Failed to insert Osoba.");
            }
        } catch (Exception e) {
            this.outputArea.setText("Insert failed: " + e.getMessage());
        }
    }

    private void findOsoba(String uuid) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }

        try {
            String result = this.controller.getPersonWithTestsFormatted(uuid);
            this.outputArea.setText(result);
        } catch (Exception e) {
            this.outputArea.setText("Find failed: " + e.getMessage());
        }
    }

    private void showOsobaBuckets() {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }

        String bucketInfo = this.controller.getOsobaBucketsInfo();
        this.outputArea.setText(bucketInfo);
    }

    private void insertPCRTest(String datum, String uuid, String kod, String hodnota, boolean vysledok, String poznamka) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }

        try {
            SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy");
            Date date = df.parse(datum);
            int kodTestu = Integer.parseInt(kod);
            double hodnotaTestu = Double.parseDouble(hodnota);

            boolean success = this.controller.insertPCRTest(date, uuid, kodTestu,
                    vysledok, hodnotaTestu, poznamka);
            if (success) {
                this.outputArea.setText("PCR Test inserted successfully.\nCode: " + kodTestu);
            } else {
                this.outputArea.setText("Failed to insert PCR Test.");
            }
        } catch (Exception e) {
            this.outputArea.setText("Insert failed: " + e.getMessage());
        }
    }

    private void findPCRTest(String kod) {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }

        try {
            int kodTestu = Integer.parseInt(kod);
            Object found = this.controller.findPCRTest(kodTestu);
            this.outputArea.setText(found != null ? "Found:\n" + found : "PCR Test not found.");
        } catch (Exception e) {
            this.outputArea.setText("Find failed: " + e.getMessage());
        }
    }

    private void showPCRBuckets() {
        if (!this.controller.isModelLoaded()) {
            this.outputArea.setText("Model not loaded.");
            return;
        }

        String bucketInfo = this.controller.getPCRBucketsInfo();
        this.outputArea.setText(bucketInfo);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            MainWindow2 window = new MainWindow2();
            window.setVisible(true);
        });
    }
}