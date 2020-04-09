CP=".:./lib/*"

client: Client.java
	javac Client.java

wc: WordCount.java
	javac -cp $(CP) WordCount.java

package:
	hadoop com.sun.tools.javac.Main WordCount.java
	jar cf WordCount.jar WordCount*.class

distribute:
	hadoop fs -copyFromLocal ./WordCount.jar
	hadoop fs -cp ./WordCount.jar gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/JAR

submit:
	hadoop jar WordCount.jar WordCount gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/data/ gs://dataproc-staging-us-central1-216204761685-cmnq2xp2/output/

clean:
	hadoop fs -rm WordCount.jar
	rm *.class
