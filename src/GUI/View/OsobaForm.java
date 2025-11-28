package GUI.View;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class OsobaForm extends JFrame {
    private final JTextField nameField;
    private final JTextField ageField;
    private final JButton submitButton;
    private final JTextArea outputArea;

    public OsobaForm() {
        this.setTitle("Osoba Form");
        this.setSize(400, 300);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setLayout(new GridLayout(5, 2));

        // Create form fields
        JLabel nameLabel = new JLabel("Name:");
        this.nameField = new JTextField();
        JLabel ageLabel = new JLabel("Age:");
        this.ageField = new JTextField();
        this.submitButton = new JButton("Submit");
        this.outputArea = new JTextArea();
        this.outputArea.setEditable(false);

        // Add components to the frame
        this.add(nameLabel);
        this.add(this.nameField);
        this.add(ageLabel);
        this.add(this.ageField);
        this.add(this.submitButton);
        this.add(new JScrollPane(this.outputArea));

        // Add action listener for the submit button
        this.submitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                OsobaForm.this.submitData();
            }
        });
    }

    private void submitData() {
        String name = this.nameField.getText();
        String age = this.ageField.getText();

        // Validate input
        if (name.isEmpty() || age.isEmpty()) {
            this.outputArea.setText("Please fill in all fields.");
            return;
        }

        // Display the entered data
        this.outputArea.setText("Entered Data:\nName: " + name + "\nAge: " + age);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            OsobaForm form = new OsobaForm();
            form.setVisible(true);
        });
    }
}