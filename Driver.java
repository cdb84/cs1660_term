import javax.swing.*;

public class Driver{
    public static void main(String [] args){
        JFrame frame = new JFrame();

        JButton fileChooseBtn = new JButton("Choose Files");
        JButton constructBtn = new JButton("Construct Inverted Indices");
        
        fileChooseBtn.setBounds(250, 250, 100, 50);
        constructBtn.setBounds(100, 250, 100, 50);
        
        frame.add(fileChooseBtn);
        frame.add(constructBtn);

        frame.setSize(500, 500);
        frame.setLayout(null);
        frame.setVisible(true);
    }
}