package test;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TwitterTumblingWindow {

	public static void main(String[] args) throws Exception{
		
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		StreamExecutionEnvironment env = 
				StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.getConfig().setGlobalJobParameters(params);
		
		//if(!params.has())
		
		
		env.execute("Twitter Language Count");

	}

}
