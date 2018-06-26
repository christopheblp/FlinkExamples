package test;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageViews {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

		if (dataStream == null) {
			System.exit(1);
			return;
		}
		
		DataStream<Tuple2<String,Double>> averageViewStream = dataStream
				.map((String row) -> {
					String[] fields = row.split(",");
					if(fields.length == 2) {
						return new Tuple3<String,Double,Integer>(
								fields[0], //webpageid
								Double.parseDouble(fields[1]), //view time in minutes
								1 // count
								);
					}
					return null;
				})
				.keyBy(0)
				.reduce((Tuple3<String,Double,Integer> cumulative, Tuple3<String,Double,Integer> input) -> 
				new Tuple3<String, Double, Integer>(input.f0, cumulative.f1 + input.f1,cumulative.f2 + input.f2)) // input : what we want to add to the result
				.map(input -> new Tuple2<String,Double>(input.f0,input.f1/input.f2));
		
		averageViewStream.print();

		env.execute("Average Views");

	}

}
