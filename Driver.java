import javax.swing.*;

import java.awt.event.*;
import java.io.File;

class ClientInstance{
    File [] files;
    ClientInstance(File [] files){
        this.files = files;
    }
    ClientInstance(){
        this.files = new File[0];
    }
    void setFiles(File [] newFiles){
        this.files = newFiles;
    }
    String getFiles(){
        String res = "<html>";
        for(File a : files){
            res += a.getName();
            res += "<br/>";
        }
        res += "</html>";
        return res;
    }
    void connect(){
        // so in theory, we would send the files through here....
    }
}

public class Driver {
    public static void main(String [] args){
        ClientInstance client = new ClientInstance();

        JFrame frame = new JFrame();
        JLabel head = new JLabel("Load My Engine");
        JLabel fileList = new JLabel("");
        JButton fileChooseBtn = new JButton("Choose Files");
        JButton constructBtn = new JButton("Construct Inverted Indices");
        JButton searchBtn = new JButton("Search for Term");
        JButton topnBtn = new JButton("Top-N");

        int btnWidth = 250;
        fileChooseBtn.setBounds(250 - btnWidth/2, 200, btnWidth, 50);
        constructBtn.setBounds(250 - btnWidth/2, 350, btnWidth, 50);
        head.setBounds(250 - btnWidth/2, 100, btnWidth, 150);
        fileList.setBounds(250 - btnWidth/2, 220, btnWidth, 100);
        searchBtn.setBounds(250 - btnWidth/2, 250, btnWidth, 50);
        topnBtn.setBounds(250 - btnWidth/2, 350, btnWidth, 50);

        fileChooseBtn.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                JFileChooser fc = new JFileChooser();
                fc.setMultiSelectionEnabled(true);
                int returnVal = fc.showOpenDialog(frame);
                if (returnVal == JFileChooser.APPROVE_OPTION){
                    client.setFiles(fc.getSelectedFiles());
                    fileList.setText(client.getFiles());
                }
            }
        });

        constructBtn.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                client.connect();
                head.setText(
                    "<html>Engine was loaded <br/> & <br/> Inverted indicies were constructed successfully! <br/> Please Select Action"
                );
                frame.remove(fileChooseBtn);
                frame.remove(constructBtn);
                frame.remove(fileList);

                frame.add(searchBtn);
                frame.add(topnBtn);

                frame.repaint();
            }
        });
        
        frame.add(fileList);
        frame.add(head);
        frame.add(fileChooseBtn);
        frame.add(constructBtn);


        frame.setSize(500, 500);
        frame.setLayout(null);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }
}