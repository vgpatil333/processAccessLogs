import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String bootstrapServer = "127.0.0.1:9092";
		String filePath = "C:\\Downloads\\data_txt\\apache-access-log.txt";
		String topicName = "project-demo";
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer kafkaProducer = new KafkaProducer(properties);
		
		produceData(topicName, filePath, kafkaProducer);
		
	}
	
	public static void produceData(String topicName,String filePath,KafkaProducer producer) throws IOException 
	{
		List<String> data = Files.readAllLines(Paths.get(filePath));
		try
		{
			for(String line : data)
		    {
		    	producer.send(new ProducerRecord(topicName,line));
		    }
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			producer.close();
		}
	    
      
	}

}
