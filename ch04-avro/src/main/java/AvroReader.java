
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
public class AvroReader extends Configured implements Tool {

    private static Schema SCHEMA = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"WeatherRecord\"," +
                    "  \"doc\": \"A weather reading.\"," +
                    "  \"fields\": [" +
                    "    {\"name\": \"temperature\", \"type\": \"int\"}," +
                    "    {\"name\": \"stationId\", \"type\": \"string\"}" +
                    "  ]" +
                    "}"
    );

    public static class MaxTemperatureMapper
            extends AvroMapper<WeatherRecord, GenericData.Record> {
        private WeatherRecord record = new WeatherRecord();


        @Override
        public void map(WeatherRecord datum, AvroCollector<GenericData.Record> collector, Reporter reporter) throws IOException {
            record  = datum;
            System.out.println("Mapper: " + record.stationId +", "  + record.temperature +", " + record.year);
            GenericData.Record record1 = new GenericData.Record(SCHEMA);
            record1.put("temperature",record.temperature );
            record1.put("stationId",record.stationId );


            collector.collect(record1);
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
        AvroJob.setMapOutputSchema(conf, SCHEMA);

        //AvroJob.setOutputSchema(conf, SCHEMA);

        conf.setInputFormat(AvroInputFormat.class);
        conf.setOutputFormat(AvroOutputFormat.class);

        conf.setNumReduceTasks(0);
        AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroReader(), args);
        System.exit(exitCode);
    }
}
