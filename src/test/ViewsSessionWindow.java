package test;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ViewsSessionWindow {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

		if (dataStream == null) {
			System.exit(1);
			return;
		}

		// user id, webpage id, temps passé sur la page
		DataStream<Tuple3<String, String, Double>> averageViewStream = dataStream.map((String row) -> {
			String[] fields = row.split(",");
			if (fields.length == 3) {
				return new Tuple3<String, String, Double>(fields[0], fields[1], Double.parseDouble(fields[2]));
			}
			return null;
		}).returns(new TypeHint<Tuple3<String, String, Double>>() {
		}).keyBy(0, 1).window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).max(2);

		averageViewStream.print();

		env.execute("Average Views Per User per User, Per Page");

	}

}
