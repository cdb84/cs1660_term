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

	/**
	 * 
	 * @return Html-formatted string of all files under selection
	 */
	String getFiles() {
		String res = "<html>";
		for (File a : files) {
			res += a.getName();
			res += "<br/>";
		}
		res += "</html>";
		return res;
	}

	/**
	 * @return Stack of results in order of occurence, with highest- occurring at
	 *         the top of the stack
	 */
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

	/**
	 * @param term the term to search for
	 * @return ArrayList<Result> containing Result(s) that contain term
	 */
	ArrayList<Result> searchForTerm(String term) {
		ArrayList<Result> ret = new ArrayList<Result>();
		for (Result r : results) {
			if (r.word.contains(term)) {
				ret.add(r);
			}
		}
		return ret;
	}

	/**
	 * Uploads all files under selection by this client instance
	 */
	private void upload() {
		for (File f : files) {
			BlobId id = BlobId.of(bucketId, "input" + instanceId + "/" + f.getName());
			/* build a new blob for this object */
			BlobInfo info = BlobInfo.newBuilder(id).build();
			try {
				/* create the actual blob by uploading the file under selection */
				storage.create(info, Files.readAllBytes(Paths.get(f.getPath())));
			} catch (IOException e) {
				System.out.println("Could not retrieve file: " + f.getPath());
			}
			System.out.println("Uploaded " + f.getPath());
		}

	}

	/**
	 * Checks to see if temporary files exist on the output blob for this job
	 * 
	 * @return true if files containing "temporary" exist, false if otherwise
	 */
	private boolean checkBucketForTemporaryFiles() {
		/* retrieve a list of blobs */
		blobs = storage.list(bucketId, BlobListOption.prefix("output" + instanceId));
		Iterator<Blob> iterator = blobs.iterateAll().iterator();
		/* try to grab the first element (may not exist) */
		try {
			iterator.next();
		} catch (Exception e) {
			return true;
		}
		while (iterator.hasNext()) {
			Blob blob = iterator.next();
			/* grab each blob and check if the name contains "temporary" */
			if (blob.getName().contains("temporary")) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Takes all output blobs and merges them into one comprehensive list of results
	 * accessible by this.results();
	 */
	private void mergeBlobs() {
		/* allOutput will hold byte representations of output files */
		ArrayList<byte[]> allOutput = new ArrayList<byte[]>();
		/* singleton files will be loaded into this stream on a per-file basis */
		ByteArrayOutputStream temporaryByteStream = new ByteArrayOutputStream();
		Iterator<Blob> iterator = blobs.iterateAll().iterator();
		iterator.next();
		while (iterator.hasNext()) {
			Blob blob = iterator.next();
			blob.downloadTo(temporaryByteStream);
			allOutput.add(temporaryByteStream.toByteArray());
			temporaryByteStream.reset();
		}
		/*
		 * for all byte-representations of output files, we will dissect the file by
		 * line in order to delimit it by tabs in to a Result structure
		 */
		for (byte[] outputFile : allOutput) {
			String outputString = new String(outputFile);
			String[] lines = outputString.split("[\\r\\n]+");
			for (String line : lines) {
				String[] items = line.split("[\\t]");
				try {
					results.add(new Result(items[0], items[1], Integer.parseInt(items[2])));
				} catch (ArrayIndexOutOfBoundsException a) {
					System.err.println("Bad index on " + line);
				}

			}
		}

	}

	/**
	 * Connects to a GCP cloud cluster and submits a hadoop job pertaining to the
	 * files under selection
	 * 
	 * @throws IOException if credentials.json is not in the current working
	 *                     directory
	 */
	void connect() throws IOException {
		/* create a psuedo-random instance id for tracking output and input files */
		instanceId = (int) (Math.random() * 1000000000);
		outputArg = bucketRoot + "output" + instanceId;
		inputArg = bucketRoot + "input" + instanceId;
		InputStream is = this.getClass().getResourceAsStream("/credentials.json");

		/*
		 * instantiate credentials so that we don't need to do this everytime we access
		 * the API
		 */
		credentials = GoogleCredentials.fromStream(is)
				.createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));

		HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
		Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer).build();

		/*
		 * instantiate a storage controller so that we don't need to do this every time
		 * either
		 */
		storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
		/* upload all files under selection by the client */
		upload();

		/* submit the job */
		dataproc.projects().regions().jobs()
				.submit(projectId, region,
						new SubmitJobRequest().setJob(new Job().setPlacement(new JobPlacement().setClusterName(clusterId))
								.setHadoopJob(new HadoopJob().setMainClass("WordCount")
										.setJarFileUris(ImmutableList.of(bucketRoot + "JAR/WordCount.jar"))
										.setArgs(ImmutableList.of(inputArg, outputArg)))))
				.execute();
		System.out.println("Submitted job for instance #" + instanceId);
		int x = 0;
		/*
		 * we will essentially wait until there are no more temporary files, at which
		 * point we will merge them together when the job is completed
		 */
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
				/*
				 * unfortunately we need to generate a stack every time so we can pop from it
				 * without it going empty on us (we can do multiple Top-N searches)
				 */
				Stack<Result> results = client.topN();
				String resultsString = "";
				int n = Integer.parseInt(editTextArea.getText());
				/* only pop the first "n" terms of the stack */
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