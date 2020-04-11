import javax.swing.*;

import java.awt.event.*;
import java.io.File;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

class ClientInstance{
	public static final String BASEURL = "https://dataproc.googleapis.com";
	File [] files;
	String apiKey;
	ClientInstance(File [] files, String apiKey){
		this.files = files;
		this.apiKey = apiKey;
	}
	ClientInstance(String apiKey){
		this.files = new File[0];
		this.apiKey = apiKey;
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
	void connect() throws IOException {
		// fuck this, I'm just going to use SSH to send a job through. This is getting too fucking ridiculous 
		// so in theory, we would send the files through here....
		URL url = new URL(BASEURL+"/v1/projects/cloud-computing-term-project/regions/us-central1/jobs:submit?key="+apiKey);
		OutputStream output;
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/json");
		output = conn.getOutputStream();

		String jsonInput = "{\"projectId\": \"cloud-computing-term-project\",\"job\": {  \"placement\": {	\"clusterName\": \"cluster-a6a9\"  },  \"statusHistory\": [],  \"reference\": {	\"jobId\": \"job-d29c0d8c\",	\"projectId\": \"cloud-computing-term-project\"  },  \"hadoopJob\": {	\"properties\": {},	\"jarFileUris\": [	  \"gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/JAR/WordCount.jar\"	],	\"args\": [	  \"gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/data\",	  \"gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/output\"	],	\"mainClass\": \"WordCount\"  }}}";

		output.write(jsonInput.getBytes());
		output.flush();

		if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED){
			throw new RuntimeException("Connecting to the GCP API failed: "+conn.getResponseCode()+": "+conn.getResponseMessage());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		String response;
		while((response = br.readLine()) != null){
			System.out.println(response);
		}

		conn.disconnect();
	}
}

public class Client {
	public static void main(String [] args){
		if(args.length < 1){
			System.err.println("Invalid arguments:\njava Client <GCP API KEY>");
			System.exit(-1);
		}
		ClientInstance client = new ClientInstance(args[0]);

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
				try {
					client.connect();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
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

		searchBtn.addActionListener(new ActionListener(){
			public void actionPerformed(ActionEvent e){
				// leads to updates for the search function
				return;
			}
		});

		topnBtn.addActionListener(new ActionListener(){
			public void actionPerformed(ActionEvent e){
				return;
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