package GUI.Controller;

import GUI.Model.Model;
import javax.swing.*;

public class MainController {
    private Model model;

    public MainController() {
        // Start with empty model, files will be loaded later
        model = null;
    }

    public void createNewOsobaFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Select Folder for Osoba Files");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
            String folderPath = fileChooser.getSelectedFile().getAbsolutePath() + "/osoba_data";

            // Ask for configuration
            String bucketsStr = JOptionPane.showInputDialog("Initial number of buckets (power of 2):", "4");
            String primarySizeStr = JOptionPane.showInputDialog("Primary block size:", "512");
            String overflowSizeStr = JOptionPane.showInputDialog("Overflow block size:", "256");

            try {
                int initialBuckets = Integer.parseInt(bucketsStr);
                int blockSizePrimary = Integer.parseInt(primarySizeStr);
                int blockSizeOverflow = Integer.parseInt(overflowSizeStr);

                if (model == null) {
                    // Create new model with PCR folder in same location
                    String pcrFolder = folderPath.replace("osoba_data", "pcr_data");
                    model = new Model(folderPath, pcrFolder,
                            initialBuckets, blockSizePrimary, blockSizeOverflow);
                } else {
                    model.createNewOsobaFile(folderPath, initialBuckets,
                            blockSizePrimary, blockSizeOverflow);
                }

                JOptionPane.showMessageDialog(null,
                        "Osoba file created in: " + folderPath,
                        "Success", JOptionPane.INFORMATION_MESSAGE);
            } catch (Exception e) {
                JOptionPane.showMessageDialog(null,
                        "Error creating file: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    public void openOsobaFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Open Osoba File Folder");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
            String folderPath = fileChooser.getSelectedFile().getAbsolutePath();

            try {
                if (model == null) {
                    model = new Model();
                }
                model.openOsobaFile(folderPath);

                JOptionPane.showMessageDialog(null,
                        "Osoba file opened from: " + folderPath,
                        "Success", JOptionPane.INFORMATION_MESSAGE);
            } catch (Exception e) {
                JOptionPane.showMessageDialog(null,
                        "Error opening file: " + e.getMessage(),
                        "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    // Similar methods for PCR files...

    public void close() {
        if (model != null) {
            model.close();
        }
    }

    public Model getModel() {
        return model;
    }
}