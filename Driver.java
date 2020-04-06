import javax.swing.*;
import java.awt.event.*;
import java.io.File;
import java.util.ArrayList;

public class Driver {
    public static void main(String [] args){
        ArrayList<File> files = new ArrayList<File>();

        JFrame frame = new JFrame();

        JButton fileChooseBtn = new JButton("Choose Files");
        JButton constructBtn = new JButton("Construct Inverted Indices");
        int btnWidth = 250;
        fileChooseBtn.setBounds(250 - btnWidth/2, 200, btnWidth, 50);
        constructBtn.setBounds(250 - btnWidth/2, 300, btnWidth, 50);

        fileChooseBtn.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                JFileChooser fc = new JFileChooser();
                fc.setMultiSelectionEnabled(true);
                int returnVal = fc.showOpenDialog(frame);
                if (returnVal == JFileChooser.APPROVE_OPTION){
                    File [] get = fc.getSelectedFiles();
                    for(int i = 0; i < get.length; i++){
                        files.add(get[i]);
                    }
                }
            }
        });
        
        frame.add(fileChooseBtn);
        frame.add(constructBtn);

        frame.setSize(500, 500);
        frame.setLayout(null);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }
}