/*
* Copyright 2017 AMSTECH INCORPORATION PRIVATE LIMITED.
* (www.amstechinc.com)
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */
 
package amstech.demo.electric.power.analysis.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodicAnalysisDriver extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(PeriodicAnalysisDriver.class);
    
    public static final int OUTLIER = -1;
    
    public static final String GUI_DATE_FORMAT = "dd/MM/yyyy";
    public static final String INPUT_DATE_FORMAT = "dd/MM/yyyy HH:mm:ss";
    
    public static final String PERIOD_TYPE = "period-type";
    public static final String START_DATE = "start-date";
    public static final String END_DATE = "end-date";
    
    
    public static final String PERIOD_TYPE_HOURLY = "hourly";
    public static final String PERIOD_TYPE_WEEKLY = "weekly";
    public static final String PERIOD_TYPE_MONTHLY = "monthly";
    public static final String PERIOD_TYPE_YEARLY = "yearly";

    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Args: <period-type> <start-date> <end-date> <input-path> <output-path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "PeriodicAnalysisDriver");

        job.setJarByClass(PeriodicAnalysisDriver.class);
        Configuration configuration = job.getConfiguration();
        configuration.set(PERIOD_TYPE, args[0]);
        configuration.set(START_DATE, args[1]);
        configuration.set(END_DATE, args[2]);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PeriodicAnalysisMapper.class);
        job.setReducerClass(PeriodicAnalysisReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.out.println("### PeriodicAnalysisDriver Started");
        PeriodicAnalysisDriver driver = new PeriodicAnalysisDriver();
        int result = ToolRunner.run(driver, args);
        System.out.println("### PeriodicAnalysisDriver Finished:" + result);
        System.out.println(result);

    }
}



