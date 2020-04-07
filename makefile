CP=".:./lib/*"

client: Client.java
	javac Client.java

wc: WordCount.java WordCountMapper.java WordCountReducer.java
	javac -cp $(CP) WordCountMapper.java
	javac -cp $(CP) WordCountReducer.java
	javac -cp $(CP) WordCount.java