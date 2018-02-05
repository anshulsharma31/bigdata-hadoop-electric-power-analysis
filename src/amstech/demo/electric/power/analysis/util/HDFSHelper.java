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
 
package amstech.demo.electric.power.analysis.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HDFSHelper {

    public static InputStream hdfsFileInputStream(String hadoopURL) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(hadoopURL);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        System.out.println(inputStream.available());
        return inputStream;
    }

    public static List<String> hdfsFileData(final String hdfsURL) throws Exception {

        Configuration conf = new Configuration();
        Path tablePath = new Path(hdfsURL);
        FileSystem fs = tablePath.getFileSystem(conf);
        FileStatus[] fileStatus = fs.listStatus(tablePath);

        //4. Using FileUtil, getting the Paths for all the FileStatus
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        List<String> rowList = new ArrayList<>();
        for (Path path : paths) {
            if (path.getName().endsWith("/_SUCCESS")) {
                continue;
            }
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader fr = null;
            try {
                fr = new BufferedReader(new InputStreamReader(inputStream));
                String line = null;
                while ((line = fr.readLine()) != null) {
                    rowList.add(line);
                }
            } finally {
                if (fr != null) {
                    fr.close();
                }
            }
        }
        return rowList;
    }

}
