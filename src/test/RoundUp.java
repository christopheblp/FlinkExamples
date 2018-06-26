package test;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RoundUp {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> dataStream = env.socketTextStream("localhost", 9999).filter(x -> {
			try {
				Double.parseDouble(x.trim());
				return true;
			} catch (Exception e) {
			}
			return false;
		}).map(x-> Math.round(Double.parseDouble(x.trim())));

		dataStream.print();

		env.execute("Modulo Operator");

	}

}
