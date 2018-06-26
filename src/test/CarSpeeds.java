package test;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CarSpeeds {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

		if (dataStream == null) {
			System.exit(1);
			return;
		}

		DataStream<String> averageViewStream = dataStream.map((String row) -> Tuple2.of(1, Double.parseDouble(row)))
				.keyBy(0).flatMap((Tuple2<Integer, Double> input, Collector<String> out) -> {
					ValueState<Tuple2<Integer, Double>> countSumState = null;
					Tuple2<Integer, Double> currentCountSum = countSumState.value();
					if (input.f1 >= 65) {
						out.collect(String.format(
								"Exceeded ! The average speed of the last %s car(s) was %s," + " your speed is %s",
								currentCountSum.f0, currentCountSum.f1 / currentCountSum.f0, input.f1));
						countSumState.clear();
					} else {
						out.collect("Thanks for staying under the speed limit !");
					}
					currentCountSum.f0 += 1;

					currentCountSum.f1 += input.f1;

					countSumState.update(currentCountSum);
				});

		env.execute("Count Window");

	}

	public static class AverageSpeedValueState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

		private transient ValueState<Tuple2<Integer, Double>> countSumState;

		@Override
		public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {
			Tuple2<Integer, Double> currentCountSum = countSumState.value();
			if (input.f1 >= 65) {
				out.collect(String.format(
						"Exceeded ! The average speed of the last %s car(s) was %s," + " your speed is %s",
						currentCountSum.f0, currentCountSum.f1 / currentCountSum.f0, input.f1));
				countSumState.clear();
			} else {
				out.collect("Thanks for staying under the speed limit !");
			}
			currentCountSum.f0 += 1;

			currentCountSum.f1 += input.f1;

			countSumState.update(currentCountSum);
		}
		
		public void open(Configuration config) {
			ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = 
					new ValueStateDescriptor<Tuple2<Integer, Double>>
						("carAverageSpeed", TypeInformation.of(new TypeHint<Tuple2<Integer,Double>>() {
						}),Tuple2.of(0, 0.0));
			
			countSumState = getRuntimeContext().getState(descriptor);
		}

	}

}
