package GUI.View;

import DS.Block;
import DS.LinearHashFile;
import javax.swing.*;
import java.awt.*;
import java.util.List;

public class BlockView extends JFrame {
    private final LinearHashFile<?> linearHashFile;

    public BlockView(LinearHashFile<?> linearHashFile) {
        this.linearHashFile = linearHashFile;
        this.initializeUI();
    }

    private void initializeUI() {
        this.setTitle("Block Information");
        this.setSize(400, 300);
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        this.setLayout(new BorderLayout());

        JTextArea textArea = new JTextArea();
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        this.add(scrollPane, BorderLayout.CENTER);

        JButton refreshButton = new JButton("Refresh");
        refreshButton.addActionListener(e -> this.displayBlockInfo(textArea));
        this.add(refreshButton, BorderLayout.SOUTH);

        this.displayBlockInfo(textArea);
    }

    private void displayBlockInfo(JTextArea textArea) {
        StringBuilder info = new StringBuilder();
        int numberOfBuckets = this.linearHashFile.getNumberOfBuckets();
        info.append("Number of Buckets: ").append(numberOfBuckets).append("\n\n");

        for (int i = 0; i < numberOfBuckets; i++) {
            int bucketPointer = this.linearHashFile.getBucketPointer(i);
            info.append("Bucket ").append(i).append(":\n");
            if (bucketPointer == -1) {
                info.append("  - Empty\n");
            } else {
                Block<?> block = this.linearHashFile.getBucket(i);
                info.append("  - Block Index: ").append(bucketPointer).append("\n");
                info.append("  - Valid Records: ").append(block.getValidCount()).append("\n");
                info.append("  - Next Block Index: ").append(block.getNextBlockIndex()).append("\n");
                info.append("  - Records:\n");
                for (int j = 0; j < block.getValidCount(); j++) {
                    info.append("    - ").append(block.getRecordAt(j).toString()).append("\n");
                }
            }
            info.append("\n");
        }

        textArea.setText(info.toString());
    }
}