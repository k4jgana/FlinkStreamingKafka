/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hello_world;


import hello_world.model.pojo.Signal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.DoubleSummaryStatistics;
import java.util.Properties;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String topic = "sensors";
		String bootStrapServers = "localhost:9092";

		FlinkKafkaConsumer<Signal> flinkKafkaConsumer=createConsumer(topic,bootStrapServers);
		DataStream<Signal> dataStream=env.addSource(flinkKafkaConsumer)
				.assignTimestampsAndWatermarks(new SignalTimestampAndWatermarkStrategy());
		KeyedStream<Signal,String> keyedStream=dataStream.keyBy(Signal::getKey);
//		DataStream<Signal> source=dataStream.filter(element-> element.getFieldValue("key").equalsIgnoreCase("A"));

//		SingleOutputStreamOperator<Signal> signalDataStream=dataStream.map(Signal::createSignal);






//		PRVA ZADACHA
		DataStream<String> result1=keyedStream
				.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
				.apply(new WindowFunction<Signal, String, String, TimeWindow>() {


					@Override
					public void apply(String key, TimeWindow window, Iterable<Signal> windowData, Collector<String> out) throws Exception {
						DoubleSummaryStatistics statistics=new DoubleSummaryStatistics();
						windowData.forEach(signal -> statistics.accept(signal.getValue()));
						out.collect(String.format("Number of occurences for the key %s in the interval [%d, %d] is %s",
								key,window.getStart(),window.getEnd(),statistics.getCount()));
					}
				});

//		PRVA ZADACHA DRUGO RESENIE
		SingleOutputStreamOperator<String> result11=keyedStream
				.window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(1)))
				.aggregate(new SignalCountingAggregateFunction());




//		VTORA ZADACHA
		DataStream<String> result2=keyedStream
						.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
								.apply(new WindowFunction<Signal, String, String, TimeWindow>() {


									@Override
									public void apply(String key, TimeWindow window, Iterable<Signal> windowData, Collector<String> out) throws Exception {
										DoubleSummaryStatistics statistics=new DoubleSummaryStatistics();
										windowData.forEach(signal -> statistics.accept(signal.getValue()));
										out.collect(String.format("Statistics for the key %s in the interval [%d, %d] is %s",
												key,window.getStart(),window.getEnd(),statistics.toString()));
									}
								});





		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */


		// execute program

//		result2.print();
//
//		result1.print();

		FlinkKafkaProducer<String> flinkKafkaProducer1=createProducer("result1",bootStrapServers);
		FlinkKafkaProducer<String> flinkKafkaProducer2=createProducer("result2",bootStrapServers);
		result1.addSink(flinkKafkaProducer1);
		result1.addSink(flinkKafkaProducer2);


		env.execute(StreamingJob.class.getName());
	}

	public static FlinkKafkaConsumer<Signal> createConsumer(String topic,String kafkaAddress) {
		Properties props=new Properties();
		props.setProperty("bootstrap.servers",kafkaAddress);
		FlinkKafkaConsumer<Signal> consumer =new FlinkKafkaConsumer<>(topic,new InputMessageDeserializationSchema(),props);
		return consumer;

	}
	public static FlinkKafkaProducer<String> createProducer(String topic, String kafkaAddress) {
		Properties props=new Properties();
		props.setProperty("bootstrap.servers",kafkaAddress);
		FlinkKafkaProducer<String> producer =new FlinkKafkaProducer<>(topic, new InputMessageSerializationSchema(), props);
		return producer;

	}
}
