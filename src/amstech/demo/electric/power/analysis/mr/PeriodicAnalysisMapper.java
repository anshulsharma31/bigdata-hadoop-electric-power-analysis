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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeriodicAnalysisMapper extends Mapper<Object, Text, IntWritable, Text> {

    public static final String DELIMITER = ";";

    private Date startDate;
    private Date endDate;
    private String periodType;

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        try {
            if (startDate == null || endDate == null || periodType == null) {
                Configuration configuration = context.getConfiguration();

                periodType = configuration.get(PeriodicAnalysisDriver.PERIOD_TYPE);

                SimpleDateFormat sdf = new SimpleDateFormat(PeriodicAnalysisDriver.GUI_DATE_FORMAT);

                String startDateStr = configuration.get(PeriodicAnalysisDriver.START_DATE);
                startDate = sdf.parse(startDateStr);

                String endDateStr = configuration.get(PeriodicAnalysisDriver.END_DATE);
                endDate = sdf.parse(endDateStr);

            }

            String line = value.toString();
            if (StringUtils.isNotBlank(line) && !line.startsWith("Date;")) {

                SimpleDateFormat sdf = new SimpleDateFormat(PeriodicAnalysisDriver.INPUT_DATE_FORMAT);

                String[] split = line.split(DELIMITER);
                if (split.length >= 2) {
                    String dateTimeStr = split[0] + " " + split[1]; // date + time
                    Date dateTime = sdf.parse(dateTimeStr);
                    if (dateTime.after(startDate) && dateTime.before(endDate)) {

                        if (split.length >= 9) {
                            int rowKey = getKey(periodType, dateTime);
                            context.write(new IntWritable(rowKey), value);
                        } else {
                            System.err.println("[ERROR] insufficient data in line :[" + line + "]");
                            context.write(new IntWritable(PeriodicAnalysisDriver.OUTLIER), new Text(line));
                        }
                    }

                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to parsed date due to:" + e, e);
        }
    }

    private int getKey(String periodType, Date dateTime) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);

        if (PeriodicAnalysisDriver.PERIOD_TYPE_HOURLY.equalsIgnoreCase(periodType)) {
            return cal.get(Calendar.HOUR_OF_DAY);
        } else if (PeriodicAnalysisDriver.PERIOD_TYPE_WEEKLY.equalsIgnoreCase(periodType)) {
            return cal.get(Calendar.DAY_OF_WEEK);
        } else if (PeriodicAnalysisDriver.PERIOD_TYPE_MONTHLY.equalsIgnoreCase(periodType)) {
            return cal.get(Calendar.MONTH);
        } else if (PeriodicAnalysisDriver.PERIOD_TYPE_YEARLY.equalsIgnoreCase(periodType)) {
            return cal.get(Calendar.YEAR);
        }

        return -1;
    }

//    private String getDayName(int num) {
//        switch (num) {
//            case 1:
//                return "SUNDAY";
//            case 2:
//                return "MONDAY";
//            case 3:
//                return "TUESDAY";
//            case 4:
//                return "WEDNESDAY";
//            case 5:
//                return "THURSDAY";
//            case 6:
//                return "FRIDAY";
//            case 7:
//                return "SATURDAY";
//            default:
//                throw new IllegalArgumentException("Invalid value for week-day:" + num);
//        }
//    }
}
