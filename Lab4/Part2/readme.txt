1. Run the following commands to get the pairs:

hadoop com.sun.tools.javac.Main WordOccurence.java
jar cf wo.jar WordOccurence*.class
# place the input file provided in this input folder
hadoop jar wo.jar WordOccurence pairs ~/input ~/output1
hdfs dfs –get ~/output1

2. Run the following commands to get the stripes:

hadoop com.sun.tools.javac.Main WordOccurence.java
jar cf wo.jar WordOccurence*.class
hadoop jar wo.jar WordOccurence stripes ~/input ~/output2
hdfs dfs –get ~/output2

3. Check the result in the output1 folder for pairs and output2 folder for stripes
4. The format used for pairs is [word, neighbor] count
5. The format used for stripes is word {neighbor = count...}

