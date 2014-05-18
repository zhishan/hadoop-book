input=/tmp/output # no assigned 
output=/tmp/output2
projectionSchema=WeatherRecordProjection.avsc
hadoop fs -rmr -f -skipTrash $output
hadoop jar ./target/ch04-avro-3.0-jar-with-dependencies.jar AvroReader  ${input} $output $projectionSchema


#export HADOOP_CLASSPATH=/home/hadoop/zhishan/.; hadoop jar ./target/mobile_probe_data_convert-1.0.jar com.lakala.mobile_data.URLDecoderDriver ${input}/$file $output  -libjars json-20140107.jar 

#mvn clean package -Dmaven.test.skip=true
#mvn clean package -DskipTests

#mapred.output.dir	hdfs://localhost:9000/user/zhishan/hadoop-book/ch02/output
#mapred.input.dir	hdfs://localhost:9000/user/zhishan/hadoop-book/ch02/input
