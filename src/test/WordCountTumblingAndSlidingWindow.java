package test;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountTumblingAndSlidingWindow {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

		if (dataStream == null) {
			System.exit(1);
			return;
		}

		DataStream<Tuple2<String, Integer>> wordCountStream = dataStream
				.flatMap((String sentence, Collector<Tuple2<String, Integer>> out) -> {
					for (String word : sentence.split(" "))
						out.collect(new Tuple2<String, Integer>(word, 1));
				}).returns(new TypeHint<Tuple2<String, Integer>>() {
				}).keyBy(0)
				// .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10))).sum(1);

		wordCountStream.print();

		env.execute("Tumbling and Sliding Window");

	}

}
