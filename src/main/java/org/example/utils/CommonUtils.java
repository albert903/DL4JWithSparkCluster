/*******************************************************************************
 * Copyright (c) 2015-2019 Skymind, Inc.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 ******************************************************************************/

package org.example.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import org.example.listeners.SparkScoreIterationListener;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description: The clusters've two nodes, one is master node that its domain name is "cluster1" , the domain name of the slave node is "cluster2"
 * @author wangfeng
 */
public class CommonUtils {
    public static final String SERVER_PATH = "hdfs://mycluster";

    public static final String TRAIN_HDFS_PATH = SERVER_PATH + "/user/root/animals/animals_split/train";
    public static final String VALIDATE_HDFS_PATH = SERVER_PATH + "/user/root/animals/animals_split/val";

    public static FileSystem openHdfsConnect() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", SERVER_PATH);
        FileSystem fs = null;
        try {
            fs = FileSystem.newInstance(new URI(SERVER_PATH),conf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fs;
    }
    public static void closeHdfsConnect(FileSystem fs) {
        try {
            if (fs != null) {
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JavaSparkContext createConf() {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("animalClass")
                .set("spark.kryo.registrator", "org.nd4j.kryo.Nd4jRegistrator")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{SparkScoreIterationListener.class});
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        return sc;
    }
}
