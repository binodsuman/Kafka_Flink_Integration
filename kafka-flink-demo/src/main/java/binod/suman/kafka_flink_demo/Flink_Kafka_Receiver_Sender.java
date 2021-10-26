package binod.suman.kafka_flink_demo;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Flink_Kafka_Receiver_Sender {
	
	public static void main(String[] args) throws Exception {
		
		String server = "localhost:9092";
		 String inputTopic = "testtopic";
		 String outputTopic = "testtopic_output";
		 
		    
		 StramStringOperation(server,inputTopic,outputTopic );
		
	}
	
	public static class StringCapitalizer implements MapFunction<String, String> {
	    @Override
	    public String map(String data) throws Exception {
	        return data.toUpperCase();
	    }
	}
	
	public static void StramStringOperation(String server,String inputTopic, String outputTopic ) throws Exception {
	    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
	    FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
	    FlinkKafkaProducer011<String> flinkKafkaProducer = createStringProducer(outputTopic, server);
	    DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
	  
	    stringInputStream.map(new StringCapitalizer()).addSink(flinkKafkaProducer);
	   
	    environment.execute();
	}
	
	public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(
			  String topic, String kafkaAddress) {
			 
			    Properties props = new Properties();
			    props.setProperty("bootstrap.servers", kafkaAddress);
			    //props.setProperty("group.id",kafkaGroup);
			    FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
			      topic, new SimpleStringSchema(), props);

			    return consumer;
	}
	
	public static FlinkKafkaProducer011<String> createStringProducer(
			  String topic, String kafkaAddress){

			    return new FlinkKafkaProducer011<>(kafkaAddress,
			      topic, new SimpleStringSchema());
	}


}
