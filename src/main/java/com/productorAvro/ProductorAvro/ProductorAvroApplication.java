package com.productorAvro.ProductorAvro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@SpringBootApplication
public class ProductorAvroApplication {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "127.0.0.1:9092");
		props.setProperty("acks", "1");
		props.setProperty("retries.config", "10");
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("schema.registry.url", "http://127.0.0.1:8081");

		SchemaRegistryClient schemaRegistryClient;

		SchemaRegistryClient registryClient = new CachedSchemaRegistryClient("http://127.0.0.1:8081",10);
		SchemaMetadata latestSchemaMetadata;
		Schema avroSchema = null;
		try {
			// getLatestSchemaMetadata takes the subject name which is topic-value format where "-value" is suffixed to topic
			// so if topic is "test-avro" then subject is "test-avro-value"
			latestSchemaMetadata = registryClient.getLatestSchemaMetadata("TP-AVRO-VENTAS-value");
			avroSchema = new Schema.Parser().parse(latestSchemaMetadata.getSchema());


			Schema schema = new Schema.Parser().parse(avroSchema.toString());
			GenericRecord record = new GenericData.Record(schema);
			//Recupera nivel de request
			GenericRecord req = new GenericData.Record(schema.getField("request").schema());
			//Recupera nivel de request transaccion
			Schema schemaR = new Schema.Parser().parse(req.getSchema().toString());
			GenericRecord reqTran = new GenericData.Record(schemaR.getField("requestTransaccion").schema());
			//Recupera nivel de request transaccion transaccion
			Schema schemaReqT = new Schema.Parser().parse(reqTran.getSchema().toString());
			GenericRecord reqTranTran = new GenericData.Record(schemaReqT.getField("requestTransaccionTransaccion").schema());
			//Recupera nivel de request transaccion transaccion atributos
			Schema schemaReqTran2 = new Schema.Parser().parse(reqTranTran.getSchema().toString());
			GenericRecord reqTranTranAtri = new GenericData.Record(schemaReqTran2.getField("atributos").schema());
			//Recupera nivel de request transaccion transaccion atributos adicionales
			Schema adicionales = reqTranTranAtri.getSchema().getField("adicionales").schema().getElementType();
			List<GenericRecord> adicionalList = new ArrayList();
			GenericRecord adicional1 = new GenericData.Record(adicionales);
			//Recupera nivel de request transaccion transaccion atributos parametros
			Schema parametros = reqTranTranAtri.getSchema().getField("parametros").schema().getElementType();
			List<GenericRecord> parametrosList = new ArrayList();
			GenericRecord parametros1 = new GenericData.Record(parametros);
			//Recupera nivel de request transaccion negocio
			Schema schemaReqN = new Schema.Parser().parse(reqTran.getSchema().toString());
			GenericRecord reqTranNeg = new GenericData.Record(schemaReqN.getField("negocio").schema());
			//Ingrese valores al Generic record
			reqTranTran.put("monto","150.00");
			reqTranTran.put("idDivisa","1");
			reqTranTran.put("referencia","45818449518533088254");
			//reqTranTranAtri.put("","");
			adicional1.put("valor", "1");
			adicional1.put("idTipo", "2");
			adicionalList.add(adicional1);
			reqTranTranAtri.put("adicionales", adicionalList);
			parametros1.put("valor", "1");
			parametros1.put("idTipo", "2");
			parametros1.put("nombre", "Arturo");
			parametrosList.add(parametros1);
			reqTranTranAtri.put("parametros", parametrosList);
			reqTranTran.put("atributos",reqTranTranAtri);
			reqTran.put("idSucursal",1);
			reqTran.put("idSubsidiaria",1);
			reqTran.put("idUsuario","USRSUPRAPP");
			reqTran.put("idNegocio",5);
			reqTran.put("terminal","1.1.1.1");
			reqTranNeg.put("sicuTienda","");
			reqTranNeg.put("sicuColaborador","");
			reqTran.put("requestTransaccionTransaccion", reqTranTran);
			reqTran.put("negocio", reqTranNeg);
			req.put("requestTransaccion",reqTran);
			record.put("request", req);

			KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<String, GenericRecord>(props);
			//prepare the kafka record
			ProducerRecord<String, GenericRecord> record1 = new ProducerRecord<>("TP-AVRO-VENTAS", null, record);
			kafkaProducer.send(record1);
			//ensures record is sent before closing the producer
			kafkaProducer.flush();

			kafkaProducer.close();


			System.out.println("Schema {}: " + avroSchema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("IO Exception while obtaining registry data");
			e.printStackTrace();
		} catch (RestClientException e) {
			// TODO Auto-generated catch block
			System.out.println("Client Exception while obtaining registry data");
			e.printStackTrace();
		}

		// Printing avro schema obtained
		System.out.println("---------------- Avro schema ----------- " + avroSchema.toString());
	}
}
