package test;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterStrings {

	public static void main(String[] args) throws Exception {

		// Permet d'obtenir le bon environnement pour notre application (local or
		// remote)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> dataStream = env.socketTextStream("localhost", 9999).filter(x -> {
			try {
				Double.parseDouble(x.trim());
				return true;
			} catch (Exception e) {
			}
			return false;
		});

		dataStream.print();

		env.execute("FilterStrings Strings");

	}

}
