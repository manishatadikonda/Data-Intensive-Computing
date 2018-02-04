1. Run the following commands to get the wordcount:

hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
# place the input file provided in this input folder
hadoop jar wc.jar WordCount ~/input ~/output
hdfs dfs –get ~/output

2. Get the output from the output folder and use it to generate the wordcloud using the ancilliary code.