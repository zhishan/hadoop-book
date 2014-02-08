hadoop fs -rmr -skipTrash /user/$USER/hadoop-book/ch02/output
hadoop jar job.jar  MaxTemperature /user/$USER/hadoop-book/ch02/input /user/$USER/hadoop-book/ch02/output
