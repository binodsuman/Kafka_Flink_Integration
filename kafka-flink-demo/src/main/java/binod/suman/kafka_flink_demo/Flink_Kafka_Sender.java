package binod.suman.kafka_flink_demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Flink_Kafka_Sender {
	
	public static void main(String[] args) throws Exception {
		 String outputTopic = "testtopic_output";
		 String server = "localhost:9092";
		    
		 StramSender(outputTopic, server);
	}
	
	public static void StramSender(String outputTopic, String server) throws Exception {
	    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
	
	    DataStream<String> stringOutputStream = environment.addSource(new StreamGenerator());
	    
	    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(
	    	      outputTopic, server);

	   
	    stringOutputStream.addSink(flinkKafkaProducer);
	   
	    environment.execute();
	}
	
	
	
	public static class StreamGenerator implements SourceFunction<String> {

		boolean flag = true;
		
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			int counter = 0;
			while(flag) {
				ctx.collect("From Flink : "+ counter++);
				System.out.println("From Flink : "+ counter);
				Thread.sleep(1000);
			}
			ctx.close();
		}

		@Override
		public void cancel() {
			flag = false;
			
		}
		
	}
	
	public static FlinkKafkaProducer011<String> createStringProducer(
			  String topic, String kafkaAddress){

			    return new FlinkKafkaProducer011<>(kafkaAddress,
			      topic, new SimpleStringSchema());
	}

}
