
Command to put input files in the desired path: 
hadoop fs -put file* <Input path>

Rank.java contains 4 map reduce functions
1->Termfrequency
2->Tfidf
3->Search
4->Rank

Termfrequency:
THe first map reduce function is for Termfrequency and the output of the mapreduce is stored in /home/cloudera/Desktop/Termfreqoutput.

Tfidf:
The second map reduce function is for tfidf here the termfrequenncy output is used as input and the result of mapreduce is stored in /home/cloudera/Desktop/TfidfOutput.

Search:
The third is for search where it uses tfidf output and match the total words with the query and return the filename and its weight in /home/cloudera/Desktop/SearchOutput.

Rank:
The fourth is for Rank, it just sorts the files according to the score and store the output in args[1] of line arguments.
If no arguments are passed, then it stops after finding tfidf values

DocWordCount Commands:

sudo hadoop fs -rm -r <Output folder>
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java 
jar -cvf DocWordCount.jar .
sudo hadoop jar DocWordCount.jar DocWordCount <Input path> <Output path>

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

TermFrequency Commands:

sudo hadoop fs -rm -r <Output folder>
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java 
jar -cvf TermFrequency.jar .
sudo hadoop jar TermFrequency.jar TermFrequency <Input path> <Output path>

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

TFIDF Commands:

sudo hadoop fs -rm -r <Output folder>
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java 
jar -cvf TFIDF.jar .
sudo hadoop jar TFIDF.jar TFIDF <Input path> <Output path>

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Search Commands:

sudo hadoop fs -rm -r <Output folder>
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java  (run from directory having java file).
jar -cvf Search.jar .
sudo hadoop jar Search.jar Search <Input path> <Output path> <space delimated query to search>

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Rank Commands:

sudo hadoop fs -rm -r <Output folder>
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java 
jar -cvf Rank.jar .
sudo hadoop jar Rank.jar Rank  <Input path> <Output path> <space delimated query to search>

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


Intermediate Values are stored in following paths:

TermFrequency output is stored in /home/cloudera/Desktop/Termfreqoutput

TF-IDF output is stored in /home/cloudera/Desktop/TfidfOutput

Search output is stored in /home/cloudera/Desktop/SearchOutput

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


Command to see output:

hadoop fs -cat <output path given at arg[1]>/part-r-*

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

