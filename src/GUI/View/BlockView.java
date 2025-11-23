package GUI.View;

import DS.Block;
import Tester.Osoba;

import javax.swing.*;
import java.awt.*;
import java.util.List;

public class BlockView extends JPanel {

    public BlockView() {
        this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
    }

    public void updateBlocks(List<Block<Osoba>> blocks) {

        this.removeAll();

        for (int i = 0; i < blocks.size(); i++) {
            Block<Osoba> block = blocks.get(i);

            JPanel blockPanel = new JPanel(new BorderLayout());
            blockPanel.setBorder(BorderFactory.createTitledBorder("Block " + i));

            StringBuilder header = new StringBuilder();
            header.append("validCount: ").append(block.getValidCount()).append("\n");
            header.append("Records:\n");

            for (int j = 0; j < block.getBlockFactor(); j++) {
                var rec = block.getRecordAt(j);
                header.append("[").append(j).append("] ").append(rec).append("\n");
            }

            JTextArea textArea = new JTextArea(header.toString());
            textArea.setEditable(false);

            blockPanel.add(new JScrollPane(textArea), BorderLayout.CENTER);
            this.add(blockPanel);
        }

        this.revalidate();
        this.repaint();
    }
}
