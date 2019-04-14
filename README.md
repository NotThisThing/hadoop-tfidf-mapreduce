# hadoop-tfidf-mapreduce

This code contains WordCount, Term Frequency, and TF-IDF

## How to use

### Step 1: Compile

1. Compile `.java` to `*.class` with Hadoop
   - `hadoop com.sun.tools.javac.Main -Xlint TFIDF_Cato.java`
2. Compile `*.class` to `.jar`
   - `jar cf TFIDF.jar *.class`

### Step 2: Run

1. Run `TFIDF.jar` with Hadoop
   - `hadoop jar TFIDF.jar TFIDF_Cato [Input_Files]`
     - Note: `[Input_Files]` is a location of your folder (contain text files) or location of your text file
2. This code automatically create 3 Folder: `zWordCount`, `zTermFrequency`, `zTFIDF`

### Step 3: Merge Result

1. Run `getmerge` command from Hadoop and convert it to `.txt` (you can generate it to `.csv`  with tab delimiter)
   - `hadoop fs -getmerge zWordCount WordCount.csv`
   - `hadoop fs -getmerge zTermFrequency TermFrequency.csv`
   - `hadoop fs -getmerge zTFIDF TFIDF.csv`
2. Open the result in Microsoft Excel or Excel-like software with tab-delimiter format
