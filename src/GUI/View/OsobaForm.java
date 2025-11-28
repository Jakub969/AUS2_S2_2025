package GUI.View;

import GUI.Controller.AppController;
import Tester.Osoba;

import javax.swing.*;
import java.awt.*;
import java.util.Date;
import java.util.function.Consumer;

public class OsobaForm extends JPanel {

    private final JTextField menoField;
    private final JTextField priezviskoField;
    private final JTextField uuidField;
    private final JTextField blockIndexField;
    private final JSpinner dateSpinner;
    private final JTextField bulkGenerateField;

    public OsobaForm(AppController controller, Runnable onUpdate) {

        this.setLayout(new GridLayout(2,1));

        JPanel inputs = new JPanel(new FlowLayout());

        this.menoField = new JTextField(10);
        this.priezviskoField = new JTextField(10);
        this.uuidField = new JTextField(10);
        this.blockIndexField = new JTextField(10);

        this.dateSpinner = new JSpinner(new SpinnerDateModel());
        this.dateSpinner.setEditor(new JSpinner.DateEditor(this.dateSpinner, "dd.MM.yyyy"));

        inputs.add(new JLabel("Meno:"));
        inputs.add(this.menoField);

        inputs.add(new JLabel("Priezvisko:"));
        inputs.add(this.priezviskoField);

        inputs.add(new JLabel("Dátum:"));
        inputs.add(this.dateSpinner);

        inputs.add(new JLabel("UUID:"));
        inputs.add(this.uuidField);

        inputs.add(new JLabel("BlockIndex:"));
        inputs.add(this.blockIndexField);

        this.add(inputs);

        // Buttons
        JPanel buttons = new JPanel(new FlowLayout());

        JButton insertBtn = new JButton("Insert");
        JButton deleteBtn = new JButton("Delete");
        JButton findBtn = new JButton("Find");
        JButton genBtn = new JButton("Generate One");

        // NEW: bulk generate controls
        this.bulkGenerateField = new JTextField("100", 5);    // default 100
        JButton bulkGenBtn = new JButton("Generate + Insert Many");

        insertBtn.addActionListener(e -> {
            Osoba o = new Osoba(
                    this.menoField.getText(),
                    this.priezviskoField.getText(),
                    (Date) this.dateSpinner.getValue(),
                    this.uuidField.getText()
            );
            controller.insertOsoba(o);
            onUpdate.run();
        });

        deleteBtn.addActionListener(e -> {
            Osoba o = Osoba.fromUUID(this.uuidField.getText());
            controller.deleteOsoba(o);
            onUpdate.run();
        });

        findBtn.addActionListener(e -> {
            Osoba dummy = Osoba.fromUUID(this.uuidField.getText());
            Osoba o = controller.findOsoba(dummy);
            JOptionPane.showMessageDialog(this,
                    (o == null) ? "Not found" : o.toString());
        });

        genBtn.addActionListener(e -> {
            Osoba rand = Osoba.generateRandom(System.currentTimeMillis());
            this.menoField.setText(rand.getMeno());
            this.priezviskoField.setText(rand.getPriezvisko());
            this.uuidField.setText(rand.getUUID());
            this.dateSpinner.setValue(rand.getDatumNarodenia());
        });

        bulkGenBtn.addActionListener(e -> {
            try {
                int count = Integer.parseInt(this.bulkGenerateField.getText());
                for (int i = 0; i < count; i++) {
                    Osoba rand = Osoba.generateRandom(System.currentTimeMillis());
                    controller.insertOsoba(rand);
                }
                onUpdate.run();
            } catch (NumberFormatException ex) {
                JOptionPane.showMessageDialog(this, "Invalid number.");
            }
        });

        buttons.add(insertBtn);
        buttons.add(deleteBtn);
        buttons.add(findBtn);
        buttons.add(genBtn);

        buttons.add(new JLabel("Count:"));
        buttons.add(this.bulkGenerateField);
        buttons.add(bulkGenBtn);

        this.add(buttons);
    }
}
