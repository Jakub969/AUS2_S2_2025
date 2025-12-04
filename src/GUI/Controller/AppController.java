package GUI.Controller;

import DS.LinearHashFile;
import DS.ChainedBlock;
import Interface.IRecord;
import Interface.IHashable;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

public class AppController<T extends IRecord<T> & IHashable> {
    private final LinearHashFile<T> linearHashFile;
    private JFrame frame;
    private JTextArea outputArea;
    private JButton loadButton;
    private JButton displayButton;

    public AppController(LinearHashFile<T> linearHashFile) {
        this.linearHashFile = linearHashFile;
        this.initializeUI();
    }

    private void initializeUI() {
        this.frame = new JFrame("Linear Hash File Viewer");
        this.frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.frame.setSize(600, 400);
        this.frame.setLayout(new BorderLayout());

        this.outputArea = new JTextArea();
        this.outputArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(this.outputArea);
        this.frame.add(scrollPane, BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel();
        this.loadButton = new JButton("Load Data");
        this.displayButton = new JButton("Display Buckets");
        buttonPanel.add(this.loadButton);
        buttonPanel.add(this.displayButton);
        this.frame.add(buttonPanel, BorderLayout.SOUTH);

        this.loadButton.addActionListener(new LoadDataAction());
        this.displayButton.addActionListener(new DisplayBucketsAction());

        this.frame.setVisible(true);
    }

    public LinearHashFile<T> getLinearHashFile() {
        return this.linearHashFile;
    }

    private class LoadDataAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            AppController.this.outputArea.append("Data loaded into the linear hash file.\n");
        }
    }

    private class DisplayBucketsAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            AppController.this.outputArea.setText("");
            int numberOfBuckets = AppController.this.linearHashFile.getPrimaryFile().getTotalBlocks();
            for (int i = 0; i < numberOfBuckets; i++) {
                ChainedBlock<T> block = AppController.this.linearHashFile.getPrimaryFile().getBlock(i);
                AppController.this.outputArea.append("Bucket " + i + ":\n");
                if (block != null) {
                    AppController.this.outputArea.append("  Head Block Index: " + i + "\n");
                    AppController.this.outputArea.append("  Valid Records: " + block.getValidCount() + "\n");
                    for (int j = 0; j < block.getValidCount(); j++) {
                        AppController.this.outputArea.append("    Record " + j + ": " + block.getRecordAt(j).toString() + "\n");
                    }
                } else {
                    AppController.this.outputArea.append("  No records in this bucket.\n");
                }
            }
        }
    }
}