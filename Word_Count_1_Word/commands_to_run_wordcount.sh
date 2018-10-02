  javac -classpath ../hadoop/hadoop-core-1.2.1.jar -d wordcount_classes/ WordCount.java 
  jar -cvf ./wordcount.jar -C wordcount_classes/ .
  ~/hadoop/bin/hadoop jar ./wordcount.jar org.myorg.WordCount /user/ubuntu/wordcount/input /user/ubuntu/wordcount/output-for-git-wc-1
  ~/hadoop/bin/hadoop dfs -ls /user/ubuntu/wordcount/output-for-git-wc-1
