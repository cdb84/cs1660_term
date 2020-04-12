import javax.swing.*;

import java.awt.event.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.google.api.gax.paging.Page;
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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class ClientInstance {
	File[] files;
	int instanceId;
	String outputArg;
	static final String projectId = "cloud-computing-term-project";
	static final String clusterId = "cluster-a6a9";
	static final String region = "us-central1";
	static final String bucketRoot = "gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/";
	static final String bucketId = "dataproc-staging-us-central1-216204761685-cmnq2xp2";
	static GoogleCredentials credentials;

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

	void retrieveObjects() throws Exception {

		Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build()
				.getService();
		Page<Blob> blobs = storage.list(bucketId, BlobListOption.prefix("output" + instanceId));
		Iterator<Blob> iterator = blobs.iterateAll().iterator();
		iterator.next();
		while (iterator.hasNext()) {
			Blob blob = iterator.next();
			if (blob.getName().contains("temporary")) {
				throw new Exception();
			}
			System.out.println(blob.getBlobId());
		}
	}

	void connect() throws IOException {
		instanceId = (int) (Math.random() * 1000000000);
		outputArg = bucketRoot + "output" + instanceId;
		InputStream is = this.getClass().getResourceAsStream("/credentials.json");
		credentials = GoogleCredentials.fromStream(is)
				.createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

		HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
		Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer)
				.build();

		dataproc.projects().regions().jobs().submit(projectId, region,
				new SubmitJobRequest().setJob(new Job().setPlacement(new JobPlacement().setClusterName(clusterId))
						.setHadoopJob(new HadoopJob().setMainClass("WordCount")
								.setJarFileUris(ImmutableList.of(bucketRoot + "JAR/WordCount.jar"))
								.setArgs(ImmutableList.of(bucketRoot + "data", outputArg)))))
				.execute();

		System.out.println("Submitted job for instance #" + instanceId);
		int x = 0;
		while (true) {
			try {
				retrieveObjects();
				break;
			} catch (Exception e) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				x += 1;
				if (x % 1000000 == 0) {
					System.out.print(".");
				}
			}
		}
		System.out.println();
	}
}

public class Client {
	public static void main(String[] args) {
		ClientInstance client = new ClientInstance();
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
					head.setText("Loading...");
					frame.repaint();
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