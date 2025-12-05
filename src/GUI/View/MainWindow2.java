package GUI.View;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.Date;
import java.util.function.Function;

import DS.LinearHashFile;
import DS.ChainedBlock;
import Interface.IRecord;
import Interface.IHashable;
import Data.Osoba;
import Data.PCRTest;

public class MainWindow2 extends JFrame {

    private LinearHashFile<Osoba> osobaFile;
    private LinearHashFile<PCRTest> pcrFile;
    private JTextArea outputArea;
    private JLabel osobaStatusLabel;
    private JLabel pcrStatusLabel;
    private JPanel osobaPanel;
    private JPanel pcrPanel;

    public MainWindow2() {
        initUI();
    }

    private void initUI() {
        setTitle("Hash File Management System");
        setSize(900, 700);
        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                closeAllFiles();
                System.exit(0);
            }
        });

        setLayout(new BorderLayout());

        // Menu Bar
        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("File");

        JMenuItem newOsobaFile = new JMenuItem("New Osoba File...");
        newOsobaFile.addActionListener(e -> createNewOsobaFile());

        JMenuItem newPCRFile = new JMenuItem("New PCR Test File...");
        newPCRFile.addActionListener(e -> createNewPCRFile());

        JMenuItem openOsobaFile = new JMenuItem("Open Osoba File...");
        openOsobaFile.addActionListener(e -> openOsobaFile());

        JMenuItem openPCRFile = new JMenuItem("Open PCR Test File...");
        openPCRFile.addActionListener(e -> openPCRFile());

        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.addActionListener(e -> {
            closeAllFiles();
            System.exit(0);
        });

        fileMenu.add(newOsobaFile);
        fileMenu.add(newPCRFile);
        fileMenu.addSeparator();
        fileMenu.add(openOsobaFile);
        fileMenu.add(openPCRFile);
        fileMenu.addSeparator();
        fileMenu.add(exitItem);

        menuBar.add(fileMenu);
        setJMenuBar(menuBar);

        // Main content - Tabbed pane
        JTabbedPane tabbedPane = new JTabbedPane();

        // Osoba tab
        osobaPanel = createRecordPanel("Osoba");
        tabbedPane.addTab("Osoba Records", osobaPanel);

        // PCR Test tab
        pcrPanel = createRecordPanel("PCR Test");
        tabbedPane.addTab("PCR Test Records", pcrPanel);

        // Output area
        outputArea = new JTextArea();
        outputArea.setEditable(false);
        JScrollPane outputScroll = new JScrollPane(outputArea);
        outputScroll.setPreferredSize(new Dimension(880, 200));

        // Status bar
        JPanel statusPanel = new JPanel(new GridLayout(1, 2));
        osobaStatusLabel = new JLabel("Osoba: Not loaded");
        pcrStatusLabel = new JLabel("PCR Test: Not loaded");
        osobaStatusLabel.setBorder(BorderFactory.createEtchedBorder());
        pcrStatusLabel.setBorder(BorderFactory.createEtchedBorder());
        statusPanel.add(osobaStatusLabel);
        statusPanel.add(pcrStatusLabel);

        // Layout
        add(tabbedPane, BorderLayout.CENTER);
        add(outputScroll, BorderLayout.SOUTH);
        add(statusPanel, BorderLayout.NORTH);
    }

    private JPanel createRecordPanel(String type) {
        JPanel panel = new JPanel(new BorderLayout());

        // Input fields
        JPanel inputPanel = new JPanel(new GridLayout(4, 2, 5, 5));
        inputPanel.setBorder(BorderFactory.createTitledBorder("Input Data"));

        JTextField field1 = new JTextField(20);
        JTextField field2 = new JTextField(20);
        JTextField field3 = new JTextField(20);
        JTextField field4 = new JTextField(20);

        if (type.equals("Osoba")) {
            inputPanel.add(new JLabel("Meno:"));
            inputPanel.add(field1);
            inputPanel.add(new JLabel("Priezvisko:"));
            inputPanel.add(field2);
            inputPanel.add(new JLabel("Dátum nar. (dd.MM.yyyy):"));
            inputPanel.add(field3);
            inputPanel.add(new JLabel("UUID:"));
            inputPanel.add(field4);
        } else {
            inputPanel.add(new JLabel("Dátum testu:"));
            inputPanel.add(field1);
            inputPanel.add(new JLabel("UUID pacienta:"));
            inputPanel.add(field2);
            inputPanel.add(new JLabel("Kód testu:"));
            inputPanel.add(field3);
            inputPanel.add(new JLabel("Poznámka:"));
            inputPanel.add(field4);
        }

        // Buttons
        JPanel buttonPanel = new JPanel(new FlowLayout());
        JButton insertBtn = new JButton("Insert");
        JButton findBtn = new JButton("Find");
        JButton deleteBtn = new JButton("Delete");
        JButton showBtn = new JButton("Show All");

        if (type.equals("Osoba")) {
            insertBtn.addActionListener(e -> insertOsoba(field1.getText(), field2.getText(),
                    field3.getText(), field4.getText()));
            findBtn.addActionListener(e -> findOsoba(field4.getText()));
            showBtn.addActionListener(e -> showOsobaBuckets());
        } else {
            insertBtn.addActionListener(e -> insertPCRTest(field1.getText(), field2.getText(),
                    field3.getText(), field4.getText()));
            findBtn.addActionListener(e -> findPCRTest(field3.getText()));
            deleteBtn.addActionListener(e -> deletePCRTest(field3.getText()));
            showBtn.addActionListener(e -> showPCRBuckets());
        }

        buttonPanel.add(insertBtn);
        buttonPanel.add(findBtn);
        buttonPanel.add(deleteBtn);
        buttonPanel.add(showBtn);

        panel.add(inputPanel, BorderLayout.NORTH);
        panel.add(buttonPanel, BorderLayout.CENTER);

        return panel;
    }

    private void createNewOsobaFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Select Folder for Osoba Files");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File folder = fileChooser.getSelectedFile();
            String folderPath = folder.getAbsolutePath() + File.separator + "osoba_data";

            // Ask for configuration
            String bucketsStr = JOptionPane.showInputDialog(this,
                    "Initial number of buckets (power of 2):", "4");
            String primarySizeStr = JOptionPane.showInputDialog(this,
                    "Primary block size:", "512");
            String overflowSizeStr = JOptionPane.showInputDialog(this,
                    "Overflow block size:", "256");

            try {
                int initialBuckets = Integer.parseInt(bucketsStr);
                int blockSizePrimary = Integer.parseInt(primarySizeStr);
                int blockSizeOverflow = Integer.parseInt(overflowSizeStr);

                if (osobaFile != null) {
                    osobaFile.close();
                }

                osobaFile = new LinearHashFile<>(Osoba.class, initialBuckets,
                        Osoba::getHash, folderPath, blockSizePrimary, blockSizeOverflow);

                osobaStatusLabel.setText("Osoba: " + folderPath);
                outputArea.setText("New Osoba file created in: " + folderPath + "\n");
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Error creating file: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void createNewPCRFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Select Folder for PCR Test Files");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File folder = fileChooser.getSelectedFile();
            String folderPath = folder.getAbsolutePath() + File.separator + "pcr_data";

            // Ask for configuration
            String bucketsStr = JOptionPane.showInputDialog(this,
                    "Initial number of buckets (power of 2):", "4");
            String primarySizeStr = JOptionPane.showInputDialog(this,
                    "Primary block size:", "512");
            String overflowSizeStr = JOptionPane.showInputDialog(this,
                    "Overflow block size:", "256");

            try {
                int initialBuckets = Integer.parseInt(bucketsStr);
                int blockSizePrimary = Integer.parseInt(primarySizeStr);
                int blockSizeOverflow = Integer.parseInt(overflowSizeStr);

                if (pcrFile != null) {
                    pcrFile.close();
                }

                pcrFile = new LinearHashFile<>(PCRTest.class, initialBuckets,
                        PCRTest::getHash, folderPath, blockSizePrimary, blockSizeOverflow);

                pcrStatusLabel.setText("PCR Test: " + folderPath);
                outputArea.setText("New PCR Test file created in: " + folderPath + "\n");
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Error creating file: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void openOsobaFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Open Osoba File Folder");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            File folder = fileChooser.getSelectedFile();

            // Check if it's a valid osoba folder
            File dirFile = new File(folder.getAbsolutePath() + File.separator + "directory.txt");
            File primaryFile = new File(folder.getAbsolutePath() + File.separator + "primary_data.bin");

            if (!dirFile.exists() || !primaryFile.exists()) {
                JOptionPane.showMessageDialog(this,
                        "This folder doesn't contain valid Osoba files.\n" +
                                "Expected files: directory.txt and primary_data.bin",
                        "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }

            try {
                if (osobaFile != null) {
                    osobaFile.close();
                }

                // Use default settings when opening existing file
                osobaFile = new LinearHashFile<>(Osoba.class, 4,
                        Osoba::getHash, folder.getAbsolutePath(), 512, 256);

                osobaStatusLabel.setText("Osoba: " + folder.getAbsolutePath());
                outputArea.setText("Osoba file opened from: " + folder.getAbsolutePath() + "\n");
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Error opening file: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void openPCRFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Open PCR Test File Folder");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            File folder = fileChooser.getSelectedFile();

            // Check if it's a valid PCR folder
            File dirFile = new File(folder.getAbsolutePath() + File.separator + "directory.txt");
            File primaryFile = new File(folder.getAbsolutePath() + File.separator + "primary_data.bin");

            if (!dirFile.exists() || !primaryFile.exists()) {
                JOptionPane.showMessageDialog(this,
                        "This folder doesn't contain valid PCR Test files.\n" +
                                "Expected files: directory.txt and primary_data.bin",
                        "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }

            try {
                if (pcrFile != null) {
                    pcrFile.close();
                }

                pcrFile = new LinearHashFile<>(PCRTest.class, 4,
                        PCRTest::getHash, folder.getAbsolutePath(), 512, 256);

                pcrStatusLabel.setText("PCR Test: " + folder.getAbsolutePath());
                outputArea.setText("PCR Test file opened from: " + folder.getAbsolutePath() + "\n");
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this,
                        "Error opening file: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    private void closeAllFiles() {
        if (osobaFile != null) {
            osobaFile.close();
            osobaStatusLabel.setText("Osoba: Closed");
        }
        if (pcrFile != null) {
            pcrFile.close();
            pcrStatusLabel.setText("PCR Test: Closed");
        }
    }

    // Osoba operations
    private void insertOsoba(String meno, String priezvisko, String datum, String uuid) {
        if (osobaFile == null) {
            outputArea.setText("Osoba file not loaded. Please open or create a file first.");
            return;
        }

        try {
            java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("dd.MM.yyyy");
            Date date = df.parse(datum);

            Osoba osoba = new Osoba(meno, priezvisko, date, uuid);
            osobaFile.insert(osoba);
            outputArea.setText("Osoba inserted: " + osoba + "\n");
        } catch (Exception e) {
            outputArea.setText("Insert failed: " + e.getMessage());
        }
    }

    private void findOsoba(String uuid) {
        if (osobaFile == null) {
            outputArea.setText("Osoba file not loaded.");
            return;
        }

        try {
            Osoba search = Osoba.fromUUID(uuid);
            Osoba found = osobaFile.find(search);
            outputArea.setText(found != null ? "Found: " + found : "Not found");
        } catch (Exception e) {
            outputArea.setText("Find failed: " + e.getMessage());
        }
    }

    private void showOsobaBuckets() {
        if (osobaFile == null) {
            outputArea.setText("Osoba file not loaded.");
            return;
        }

        // Implementation similar to your original showBuckets method
        outputArea.setText("Showing Osoba buckets...\n");
        // Add your bucket display logic here
    }

    // PCR Test operations (similar to Osoba operations)
    private void insertPCRTest(String datum, String uuid, String kod, String poznamka) {
        // Implement PCR test insertion
    }

    private void findPCRTest(String kod) {
        // Implement PCR test find
    }

    private void deletePCRTest(String kod) {
        // Implement PCR test delete
    }

    private void showPCRBuckets() {
        // Implement PCR buckets display
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new MainWindow2().setVisible(true);
        });
    }
}