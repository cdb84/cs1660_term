import javax.swing.*;

import java.awt.event.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;


import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class ClientInstance {
	File[] files;

	ClientInstance() {
		this.files = new File[0];
	}

	void setFiles(File[] newFiles) {
		this.files = newFiles;
	}

	String getFiles() {
		String res = "<html>";
		for (File a : files) {
			res += a.getName();
			res += "<br/>";
		}
		res += "</html>";
		return res;
	}

	void connect() throws IOException {
		InputStream is = this.getClass().getResourceAsStream("/credentials.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(is)
				.createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

		HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
		Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer)
				.build();

		dataproc.projects().regions().jobs().submit("cloud-computing-term-project", "us-central1", new SubmitJobRequest().setJob(
				new Job().setPlacement(new JobPlacement().setClusterName("cluster-a6a9")).setHadoopJob(new HadoopJob()
						.setMainClass("WordCount")
						.setJarFileUris(ImmutableList
								.of("gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/JAR/WordCount.jar"))
						.setArgs(ImmutableList.of("gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/data",
								"gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/output")))))
				.execute();
	}
}

public class Client {
	public static void main(String[] args) {
		ClientInstance client = new ClientInstance();
		try {
			client.connect();
			
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		System.exit(-1);
		JFrame frame = new JFrame();
		JLabel head = new JLabel("Load My Engine");
		JLabel fileList = new JLabel("");
		JButton fileChooseBtn = new JButton("Choose Files");
		JButton constructBtn = new JButton("Construct Inverted Indices");
		JButton searchBtn = new JButton("Search for Term");
		JButton topnBtn = new JButton("Top-N");

		int btnWidth = 250;
		fileChooseBtn.setBounds(250 - btnWidth / 2, 200, btnWidth, 50);
		constructBtn.setBounds(250 - btnWidth / 2, 350, btnWidth, 50);
		head.setBounds(250 - btnWidth / 2, 100, btnWidth, 150);
		fileList.setBounds(250 - btnWidth / 2, 220, btnWidth, 100);
		searchBtn.setBounds(250 - btnWidth / 2, 250, btnWidth, 50);
		topnBtn.setBounds(250 - btnWidth / 2, 350, btnWidth, 50);

		fileChooseBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JFileChooser fc = new JFileChooser();
				fc.setMultiSelectionEnabled(true);
				int returnVal = fc.showOpenDialog(frame);
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					client.setFiles(fc.getSelectedFiles());
					fileList.setText(client.getFiles());
				}
			}
		});

		constructBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				try {
					client.connect();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				head.setText(
						"<html>Engine was loaded <br/> & <br/> Inverted indicies were constructed successfully! <br/> Please Select Action");
				frame.remove(fileChooseBtn);
				frame.remove(constructBtn);
				frame.remove(fileList);

				frame.add(searchBtn);
				frame.add(topnBtn);

				frame.repaint();
			}
		});

		searchBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				// leads to updates for the search function
				return;
			}
		});

		topnBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
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