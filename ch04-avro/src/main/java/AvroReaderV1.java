
import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import specific.WeatherRecord;

import java.io.IOException;

/**
 * Created by zhishan on 5/18/14.
 */
public class AvroReaderV1 extends Configured implements Tool {


    public static class MaxTemperatureMapper
            extends AvroMapper<WeatherRecord, WeatherRecord> {
        private WeatherRecord record = new WeatherRecord();

        @Override
        public void map(WeatherRecord datum, AvroCollector<WeatherRecord> collector, Reporter reporter) throws IOException {
            record  = datum;
            System.out.println("Mapper: " + record.stationId +", "  + record.temperature +", " + record.year);
            record.temperature = 0;
            record.stationId = "zhishan.li";
            collector.collect(record);
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Max temperature");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setInputSchema(conf, WeatherRecord.SCHEMA$);
        AvroJob.setMapOutputSchema(conf, WeatherRecord.SCHEMA$);

        //AvroJob.setOutputSchema(conf, SCHEMA);

        conf.setInputFormat(AvroInputFormat.class);
        conf.setOutputFormat(AvroOutputFormat.class);

        conf.setNumReduceTasks(0);
        AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroReaderV1(), args);
        System.exit(exitCode);
    }
}
