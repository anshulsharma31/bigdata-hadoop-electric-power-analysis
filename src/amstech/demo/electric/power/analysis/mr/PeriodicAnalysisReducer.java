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

import static amstech.demo.electric.power.analysis.mr.PeriodicAnalysisMapper.DELIMITER;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeriodicAnalysisReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {

        if (key.get() == PeriodicAnalysisDriver.OUTLIER) {
            for (Text value : values) {
                context.write(key, value);
            }
            return;
        }

        Configuration configuration = context.getConfiguration();

        Map<String, Field> map = new LinkedHashMap<>();

        map.put("globalActivePower", new Field("globalActivePower"));
        map.put("globalReactivePower", new Field("globalReactivePower"));
        map.put("voltage", new Field("voltage"));
        map.put("globalIntensity", new Field("globalIntensity"));
        map.put("subMetering1", new Field("subMetering1"));
        map.put("subMetering2", new Field("subMetering2"));
        map.put("subMetering3", new Field("subMetering3"));

        long count = 0;

        for (Text value : values) {
            String line = value.toString();
            if (StringUtils.isNotBlank(line)) {
                String[] split = line.split(DELIMITER);
                if (split.length >= 9) {
                    String date = split[0];
                    String time = split[1];
                    double globalActivePower = Double.parseDouble(split[2]);
                    Field field = map.get("globalActivePower");
                    field.updateMinMax(globalActivePower);
                    field.updateTotal(globalActivePower);

                    double globalReactivePower = Double.parseDouble(split[3]);
                    field = map.get("globalReactivePower");
                    field.updateMinMax(globalReactivePower);
                    field.updateTotal(globalReactivePower);

                    double voltage = Double.parseDouble(split[4]);
                    field = map.get("voltage");
                    field.updateMinMax(voltage);
                    field.updateTotal(voltage);

                    double globalIntensity = Double.parseDouble(split[5]);
                    field = map.get("globalIntensity");
                    field.updateMinMax(globalIntensity);
                    field.updateTotal(globalIntensity);

                    double subMetering1 = Double.parseDouble(split[6]);
                    field = map.get("subMetering1");
                    field.updateMinMax(subMetering1);
                    field.updateTotal(subMetering1);

                    double subMetering2 = Double.parseDouble(split[7]);
                    field = map.get("subMetering2");
                    field.updateMinMax(subMetering2);
                    field.updateTotal(subMetering2);

                    double subMetering3 = Double.parseDouble(split[8]);
                    field = map.get("subMetering3");
                    field.updateMinMax(subMetering3);
                    field.updateTotal(subMetering3);

                    count++;
                } else {
                    System.err.println("[ERROR] insufficient data in line :[" + line + "]");
                }
            }
        }

        StringBuilder output = new StringBuilder();

        Field field = map.get("globalActivePower");
        field.updateAvg(count);
        output.append(field);

        field = map.get("globalReactivePower");
        field.updateAvg(count);
        output.append(";").append(field);

        field = map.get("voltage");
        field.updateAvg(count);
        output.append(";").append(field);

        field = map.get("globalIntensity");
        field.updateAvg(count);
        output.append(";").append(field);

        field = map.get("subMetering1");
        field.updateAvg(count);
        output.append(";").append(field);

        field = map.get("subMetering2");
        field.updateAvg(count);
        output.append(";").append(field);

        field = map.get("subMetering3");
        field.updateAvg(count);
        output.append(";").append(field);

        context.write(key, new Text(output.toString()));

    }

}

class Field {

    private String name;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = Double.MIN_VALUE;
    private double avgValue = 0.0;
    private double totalValue = 0.0;

    public Field() {
    }

    public Field(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return minValue + "#" + maxValue + "#" + avgValue;
    }

    public void updateMinMax(double value) {
        if (value < minValue) {
            minValue = value;
        }
        if (value > maxValue) {
            maxValue = value;
        }
    }

    public void updateTotal(double value) {
        totalValue += value;
    }

    public void updateAvg(long count) {
        avgValue = totalValue / count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public double getAvgValue() {
        return avgValue;
    }

    public void setAvgValue(double avgValue) {
        this.avgValue = avgValue;
    }

    public double getTotalValue() {
        return totalValue;
    }

    public void setTotalValue(double totalValue) {
        this.totalValue = totalValue;
    }

}
