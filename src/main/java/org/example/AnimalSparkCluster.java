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

package org.example;



import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.deeplearning4j.core.storage.StatsStorageRouter;
import org.deeplearning4j.core.storage.impl.RemoteUIStatsStorageRouter;
import org.example.datasets.DatasetIteratorFromHdfs;
import org.deeplearning4j.eval.Evaluation;
import org.example.listeners.SparkScoreIterationListener;
import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.conf.layers.ConvolutionLayer;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.conf.layers.SubsamplingLayer;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.spark.api.RDDTrainingApproach;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.api.stats.SparkTrainingStats;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster;
import org.deeplearning4j.spark.parameterserver.training.SharedTrainingMaster;
import org.deeplearning4j.spark.stats.StatsUtils;
import org.example.utils.CommonUtils;
import org.example.utils.DownloaderUtility;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.parameterserver.distributed.conf.VoidConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: The distributed training model based on spark and hadoop,the dataset is image
 * The spark and hadoop clusters've two nodes, one is master node that its domain name is "cluster1",the domain name of the slave node is "cluster2",of course,everyone can extend more nodes
 * Tip: the pom.xml configuration ->For Spark examples: change the _1 to _2 to switch between Spark 1 and Spark 2
 * running ways:
 * one way: spark on yarn--->>>
 * export the module to jar,and then execute command:spark-submit --class org.deeplearning4j.AnimalModelByHdfsSparkCluster --master yarn --deploy-mode cluster /home/out/dl4j-spark-cluster-1.0.0-beta-bin.jar
 * second way: standalone--->>>
 * export the module to jar,and then execute command:spark-submit --class org.deeplearning4j.AnimalModelByHdfsSparkCluster --master spark://cluster1:7077 --deploy-mode cluster /home/out/dl4j-spark-cluster-1.0.0-beta-bin.jar
 * you may encounter some error:NTPTimeSource: Error querying NTP server, attempt 1 of 10, reference->https://deeplearning4j.org/spark#sparkstatsntp
 * @author wangfeng
 */
public class AnimalSparkCluster {
    private static final Logger log = LoggerFactory.getLogger(AnimalSparkCluster.class);
    private static int height = 100;
    private static int width = 100;
    private static int channels = 3;
    private static long seed = 42;
    private static int averagingFrequency = 3;
    private static int batchSizePerWorker = 4;

    private static int examplesPerDataSetObject = 1;
    private static boolean trainingMode = false;
    private static String modelPath = "/user/root/models/AnimalModelByHdfsSparkClusterModel.bin";
    private static  String trainMonitorLog = "/user/root/trainlog/AnimalModelByHdfsTrainingStatsSpark.dl4j";
    private static  String trainPerformanceLog = "/user/root/trainlog/AnimalModelByHdfsSparkCluster.html";
    private static  String trainIteratorScoreLog = "/user/root/trainlog/scoreAnimalModelByHdfsSparkCluster.log";

    public static String dataLocalPath;

    public static void main(String[] args) throws  Exception{
        //dataLocalPath = DownloaderUtility.ANIMALS.Download();
        JavaSparkContext sc = CommonUtils.createConf();
        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());

        MultiLayerConfiguration conf = lenetModelConf();

        TrainingMaster tm = null;
        if (!trainingMode) {
            //Introduce & configuration for Spark training: https://deeplearning4j.org/distributed
            //SharedTraining
            VoidConfiguration voidConfiguration = VoidConfiguration.builder()
                    .unicastPort(40123)// This can be any port, but it should be open for IN/OUT comms on all Spark nodes
                    .networkMask("172.19.0.0/24")//The master node ip address is 172.19.0.2, the worker node one ip address is 172.19.0.3
                    .build();
            tm = new SharedTrainingMaster.Builder(voidConfiguration, batchSizePerWorker)
                    .updatesThreshold(1e-3)// encoding threshold. Please check https://deeplearning4j.org/distributed for details
                    .rddTrainingApproach(RDDTrainingApproach.Direct)
                    .batchSizePerWorker(batchSizePerWorker)//This controls the minibatch size for each worker.
                    .workersPerNode(1)// this option will enforce exactly 1 workers for each Spark node
                    .collectTrainingStats(true)//to collect training stats.
                    .build();
        } else {
            //Introduce & configuration  for Spark training:https://deeplearning4j.org/spark
            //ParameterAveragingTraining
            tm = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)//This is specified in the builder constructor. This value specifies how many examples are in each DataSet object.
                .workerPrefetchNumBatches(2)//Asynchronously prefetch up to 2 batches
                .averagingFrequency(averagingFrequency)//This controls how frequently the parameters are averaged and redistributed, in terms of number of minibatches of size batchSizePerWorker
                .batchSizePerWorker(batchSizePerWorker)//This controls the minibatch size for each worker.
                .build();
        }



        SparkDl4jMultiLayer sparkNetwork = new SparkDl4jMultiLayer(sc, conf, tm);
        sparkNetwork.setCollectTrainingStats(true);
        //remote monitor
        StatsStorageRouter remoteUIRouter = new RemoteUIStatsStorageRouter("http://mycluster");//tip the port conflict hadoop

        //StatsStorage statsStorage = new FileStatsStorage(new File(trainMonitorLog));
        //SparkNetwork.setListeners(remoteUIRouter, new StatsListener(statsStorage), new SparkScoreIterationListener(10));
        sparkNetwork.setListeners(remoteUIRouter, new SparkScoreIterationListener(1, trainIteratorScoreLog));
        SparkTrainingStats stats = sparkNetwork.getSparkTrainingStats();



        DataSetIterator trainIterator = new DatasetIteratorFromHdfs(batchSizePerWorker,true);

        List<DataSet> trainDataList = new ArrayList<>();
        while (trainIterator.hasNext()) {
            trainDataList.add(trainIterator.next());
        }
        JavaRDD<DataSet> trainData = sc.parallelize(trainDataList);


        MultiLayerNetwork network = null;
        for (int i = 0; i < 10; i++) {
            network = sparkNetwork.fit(trainData);
            //network = sparkNetwork.fit(CommonUtils.TRAIN_HDFS_PATH);
            log.info("Completed Epoch {}", i);
        }
        if (trainingMode) {
            StatsUtils.exportStatsAsHtml(stats, trainPerformanceLog, sc);
        }
        saveModel(fs, network);

        DataSetIterator validateIterator = new DatasetIteratorFromHdfs(batchSizePerWorker,false);
        List<DataSet> testDataList = new ArrayList<>();
        while (validateIterator.hasNext()) {
            testDataList.add(validateIterator.next());
        }
        JavaRDD<DataSet> testData = sc.parallelize(testDataList);
        Evaluation evaluation = sparkNetwork.doEvaluation(testData, 32, new Evaluation(4))[0];//Work-around for 0.9.1 bug: see https://deeplearning4j.org/releasenotes
        log.info(evaluation.stats());
    }

    public static MultiLayerConfiguration lenetModelConf() {

        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(seed)
                .l2(0.005)
                .activation(Activation.RELU)
                .weightInit(WeightInit.XAVIER)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .updater(new Nesterovs(0.0001, 0.9))
                .list()
                .layer(0, new ConvolutionLayer.Builder(new int[]{5, 5}, new int[]{1, 1}, new int[]{0, 0}).name("cnn1")
                        .nIn(channels).nOut(50).biasInit(0).build())
                .layer(1, new SubsamplingLayer.Builder(new int[]{2,2}, new int[]{2,2}).name("maxpool1").build())
                .layer(2, new ConvolutionLayer.Builder(new int[]{5,5}, new int[]{5, 5}, new int[]{1, 1}).name("cnn2")
                        .nOut(100).biasInit(0).build())
                .layer(3, new SubsamplingLayer.Builder(new int[]{2,2}, new int[]{2,2}).name("maxpool2").build())
                .layer(4, new DenseLayer.Builder().nOut(500).build())
                .layer(5, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                        .nOut(4)
                        .activation(Activation.SOFTMAX)
                        .build())
                .setInputType(InputType.convolutional(height, width, channels))
                .build();

        return conf;

    }
    public static void saveModel(FileSystem fs, Model model ) throws Exception{

       String  json = null;
        if (model instanceof MultiLayerNetwork) {
            json = ((MultiLayerNetwork)model).getLayerWiseConfigurations().toJson();
        } else if (model instanceof ComputationGraph) {
            json = ((ComputationGraph)model).getConfiguration().toJson();
        }
        assert json != null;
        byte [] bytes = json.getBytes();
        FSDataOutputStream out = fs.create(new Path(modelPath));
        out.write(bytes);
        out.hsync();
        fs.close();

    }
}
