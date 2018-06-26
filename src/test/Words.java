package test;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
public class Words {

	public static void main(String[] args) throws Exception {
		
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params);
		// show user defined parameters in the apache flink dashboard
		
		DataStream<String> dataStream;
		
		if(params.has("input")) 
		{
			System.out.println("Executing Words example with file input");
			dataStream = env.readTextFile(params.get("input"));
		}else if (params.has("host") && params.has("port")) 
		{
			System.out.println("Executing Words example with socket stream");
			dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
		}
		else
		{
			System.exit(1);
			return;
		}
		
		DataStream<String> wordDataStream = dataStream.flatMap(
				(String sentence, Collector<String> out) -> {
					for(String word: sentence.split(" "))
						out.collect(word);
		}).returns(Types.STRING);
		
		wordDataStream.print();
		
		env.execute("Word Split");		
	}

}
