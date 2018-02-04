1. Run the following commands to get the lemmas:

hadoop com.sun.tools.javac.Main Lemma_A1.java
jar cf lemma.jar Lemma_A1*.class
# place some sample tess files in this input folder
hadoop jar lemma.jar Lemma_A1 pairs ~/input ~/output3
hdfs dfs –get ~/output3


2. Check the result generated in the output folder. 
3. The format used for generating the result is 
Lemma or Word	{ docId , chapter#, line#} Count = #
