1. Run the following commands to get the lemma triplets:

hadoop com.sun.tools.javac.Main Activity2_2Words.java
jar cf latin_pairs.jar Activity2_2Words*.class
# place some sample tess files in this input folder
hadoop jar latin_pairs.jar Activity2_2Words pairs ~/input ~/output5
hdfs dfs –get ~/output5


2. Check the result generated in the output folder. 

3. The format used for generating the result is 
<Word, Neighbor1, Neighbor2>	<location>
