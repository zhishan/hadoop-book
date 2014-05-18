import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhishan on 5/18/14.
 */
public class AvroReaderTest {
    @Test
    public void testRun() throws Exception {

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        Path input = new Path("ch04-avro/src/main/resources/part-00000.avro");
        System.out.println("input path: " + input.toString());
        Path output = new Path("ch04-avro/output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        AvroReader driver = new AvroReader();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]{
                input.toString(), output.toString()
        });
        Assert.assertEquals(exitCode, 0);

    }
}
