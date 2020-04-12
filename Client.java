import javax.swing.*;

import java.awt.event.*;
import java.awt.Cursor;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class Result {
	String word;
	String document;
	int count;

	Result(String word, String document, int count) {
		this.word = word;
		this.document = document;
		this.count = count;
	}

	Result() {
		word = "";
		document = "";
		count = 0;
	}

	public String toString() {
		return word + ": " + document + ", " + count;
	}
}

class ClientInstance {
	File[] files;
	int instanceId;
	String outputArg;
	String inputArg;
	Page<Blob> blobs;
	byte[] outputData;
	ArrayList<Result> results;
	Storage storage;
	static final String projectId = "cloud-computing-term-project";
	static final String clusterId = "cluster-a6a9";
	static final String region = "us-central1";
	static final String bucketRoot = "gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/";
	static final String bucketId = "dataproc-staging-us-central1-216204761685-cmnq2xp2";
	static GoogleCredentials credentials;

	ClientInstance() {
		this.files = new File[0];
		results = new ArrayList<Result>();
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

	Stack<Result> topN() {
		Stack<Result> topN = new Stack<Result>();
		Result top = new Result();
		for (Result r : results) {
			if (r.count > top.count) {
				topN.push(r);
				top = r;
			}
		}
		return topN;
	}

	ArrayList<Result> searchForTerm(String term) {
		ArrayList<Result> ret = new ArrayList<Result>();
		for (Result r : results) {
			if (r.word.contains(term)) {
				ret.add(r);
			}
		}
		return ret;
	}

	void upload() {
		for (File f : files) {

			BlobId id = BlobId.of(bucketId, "input" + instanceId + "/" + f.getName());
			BlobInfo info = BlobInfo.newBuilder(id).build();
			try {
				storage.create(info, Files.readAllBytes(Paths.get(f.getPath())));
			} catch (IOException e) {
				System.out.println("Could not retrieve file: " + f.getPath());
			}
			System.out.println("Uploaded " + f.getPath());
		}

	}

	boolean checkBucketForTemporaryFiles() {
		blobs = storage.list(bucketId, BlobListOption.prefix("output" + instanceId));
		Iterator<Blob> iterator = blobs.iterateAll().iterator();
		try {
			iterator.next();
		} catch (Exception e) {
			return true;
		}
		while (iterator.hasNext()) {
			Blob blob = iterator.next();
			if (blob.getName().contains("temporary")) {
				return true;
			}
			System.out.println(blob.getBlobId());
		}
		return false;
	}

	void mergeBlobs() {
		ArrayList<byte[]> allOutput = new ArrayList<byte[]>();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		Iterator<Blob> iterator = blobs.iterateAll().iterator();
		iterator.next();
		while (iterator.hasNext()) {
			Blob blob = iterator.next();
			blob.downloadTo(byteStream);
			allOutput.add(byteStream.toByteArray());
			byteStream.reset();
		}
		for (byte[] outputFile : allOutput) {
			String outputString = new String(outputFile);
			String[] lines = outputString.split("[\\r\\n]+");
			for (String line : lines) {
				String[] items = line.split("[\\t]");
				try {
					results.add(new Result(items[0], items[1], Integer.parseInt(items[2])));
				} catch (ArrayIndexOutOfBoundsException a) {
					System.out.println("Bad index on " + line);
				}

			}
		}

		for (Result a : results) {
			System.out.println(a);
		}

	}

	void connect() throws IOException {
		instanceId = (int) (Math.random() * 1000000000);
		outputArg = bucketRoot + "output" + instanceId;
		inputArg = bucketRoot + "input" + instanceId;
		InputStream is = this.getClass().getResourceAsStream("/credentials.json");
		credentials = GoogleCredentials.fromStream(is)
				.createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

		HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
		Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer).build();

		storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
		upload();

		dataproc.projects().regions().jobs()
				.submit(projectId, region,
						new SubmitJobRequest().setJob(new Job().setPlacement(new JobPlacement().setClusterName(clusterId))
								.setHadoopJob(new HadoopJob().setMainClass("WordCount")
										.setJarFileUris(ImmutableList.of(bucketRoot + "JAR/WordCount.jar"))
										.setArgs(ImmutableList.of(inputArg, outputArg)))))
				.execute();
		System.out.println("Submitted job for instance #" + instanceId);
		int x = 0;
		while (checkBucketForTemporaryFiles()) {
			x++;
			if (x % 10000 == 0) {
				System.out.print(".");
			}
		}
		System.out.println();
		mergeBlobs();
	}
}

public class Client {
	public static void main(String[] args) {
		ClientInstance client = new ClientInstance();
		JFrame frame = new JFrame();
		JLabel head = new JLabel("Load My Engine");
		JLabel fileList = new JLabel("");
		JLabel searchResults = new JLabel("Search Results");
		JLabel topNResults = new JLabel("Top-N Results");
		JButton fileChooseBtn = new JButton("Choose Files");
		JButton constructBtn = new JButton("Construct Inverted Indices");
		JButton searchBtn = new JButton("Search for Term");
		JButton topnBtn = new JButton("Top-N");
		JButton goBtn = new JButton("Search");

		JTextArea editTextArea = new JTextArea("Search");

		int btnWidth = 250;
		fileChooseBtn.setBounds(250 - btnWidth / 2, 200, btnWidth, 50);
		constructBtn.setBounds(250 - btnWidth / 2, 350, btnWidth, 50);
		head.setBounds(250 - btnWidth / 2, 100, btnWidth, 150);
		searchResults.setBounds(250 - btnWidth / 2, 150, btnWidth, 300);
		topNResults.setBounds(250 - btnWidth / 2, 200, btnWidth, 300);

		fileList.setBounds(250 - btnWidth / 2, 220, btnWidth, 100);
		searchBtn.setBounds(250 - btnWidth / 2, 250, btnWidth, 50);
		topnBtn.setBounds(250 - btnWidth / 2, 350, btnWidth, 50);
		editTextArea.setBounds(250 - btnWidth / 2, 50, btnWidth, 25);
		goBtn.setBounds(250 - btnWidth / 2, 75, btnWidth, 25);
		JButton topNGoBtn = goBtn;
		topNGoBtn.setText("Select top N terms");

		fileChooseBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JFileChooser fc = new JFileChooser();
				fc.setMultiSelectionEnabled(true);
				int returnVal = fc.showOpenDialog(frame);
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					client.setFiles(fc.getSelectedFiles());
					fileList.setText(client.getFiles());
					for (File a : client.files) {
						System.out.println(a.getPath());
					}
				}
			}
		});

		constructBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				try {
					frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
					client.connect();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				frame.setCursor(Cursor.getDefaultCursor());
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
				head.setText("<html>Search functionality</html>");
				frame.remove(searchBtn);
				frame.add(editTextArea);
				frame.add(goBtn);
				frame.add(searchResults);
				frame.repaint();
			}
		});

		goBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				head.setText("");
				ArrayList<Result> results = client.searchForTerm(editTextArea.getText());
				String resultsString = "";
				int count = 0;
				for (Result r : results) {
					resultsString += r.toString() + "<br/>";
					count++;
					if (count >= 20) {
						break;
					}
				}
				searchResults.setText("<html>" + resultsString + "</html>");
				frame.repaint();
			}
		});

		topnBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				head.setText("<html>Search functionality</html>");
				frame.remove(topnBtn);
				frame.remove(searchBtn);
				frame.add(editTextArea);
				frame.add(topNGoBtn);
				frame.add(topNResults);
				frame.repaint();
			}
		});

		topNGoBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				head.setText("");
				Stack<Result> results = client.topN();
				String resultsString = "";
				int n = Integer.parseInt(editTextArea.getText());
				for (int x = 0; x < n; x++) {
					resultsString += results.pop().toString() + "<br/>";
				}
				System.out.println(resultsString);
				topNResults.setText("<html>" + resultsString + "</html>");
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