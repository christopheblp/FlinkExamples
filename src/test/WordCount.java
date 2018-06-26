package test;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

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
				}).returns(new TypeHint<Tuple2<String,Integer>>(){})
				.keyBy(0).sum(1);
		// for every word sum up the second parameter at index=1, sum up the counts

		wordCountStream.print();

		env.execute("Word Count");
	}
	

}
