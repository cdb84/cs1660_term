CP=".:./lib/*"

client: Client.java
	javac Client.java

wc: WordCount.java
	javac -cp $(CP) WordCount.java
