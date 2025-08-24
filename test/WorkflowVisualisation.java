package test;

import java.awt.EventQueue;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JTextArea;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;

public class WorkflowVisualisation {

	private JFrame frame;

	/**
	 * Launch the application.
	 */
//	public static void main(String[] args) {
	public static void lunch() {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					WorkflowVisualisation window = new WorkflowVisualisation();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public WorkflowVisualisation() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 450, 300);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		JTextArea textArea = new JTextArea();
		textArea.setFont(new Font("Monospaced", Font.BOLD, 14));
		textArea.setTabSize(12);
		textArea.setBackground(Color.WHITE);
		frame.getContentPane().add(textArea, BorderLayout.CENTER);
		
		FileReader flux= null;
		BufferedReader input= null;
		String str;
		try {
			flux= new FileReader ("E://hamdi/these/workflow.work");
			input= new BufferedReader( flux);
			while((str=input.readLine())!=null)
	        {
				textArea.append(str);
				textArea.append("\n");
	        }
			
		} catch (IOException e) {
			System.out.println("Impossible d'ouvrir le fichier : " +e.toString());
		} 
		 
        
	}

}
