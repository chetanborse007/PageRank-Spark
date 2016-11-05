# Create new input directory on HDFS and copy input files from local
# filesystem to HDFS filesystem.
hadoop fs -rm -r input/PageRank
hadoop fs -mkdir -p input/PageRank
hadoop fs -put input/* input/PageRank


# Create new output directories on HDFS and delete existing output 
# directories generated by previous execution instance of 
# Spark application.
hadoop fs -mkdir -p output/Spark/PageRank
hadoop fs -rm -r output/Spark/PageRank


# Run Spark application on HDFS.
#spark-submit --master yarn --deploy-mode client --executor-memory 1g src/WikiPageRank.py -i "input/PageRank" -o "output/Spark/PageRank/TopWikiPage" -t 10 -d 0.85 -k 100
spark-submit --master yarn --deploy-mode client src/WikiPageRank.py -i "input/PageRank" -o "output/Spark/PageRank/TopWikiPage" -t 10 -d 0.85 -k 100


# Get final output from HDFS.
rm -r PageRank/
hadoop fs -get output/Spark/PageRank

