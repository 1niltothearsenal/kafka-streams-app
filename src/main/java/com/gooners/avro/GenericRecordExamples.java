package com.gooners.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class GenericRecordExamples {

    private static  final Logger LOGGER = LoggerFactory.getLogger(GenericRecordExamples.class.getName());

    public static void main(String[] args) {

        //step 0: Define Schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"type\":\"record\",\n" +
                "    \"namespace\":\"com.example\",\n" +
                "    \"name\":\"Customer\",\n" +
                "    \"doc\":\"Avro Schema for our Customer\",\n" +
                "    \"fields\":[\n" +
                "        {\"name\":\"first_name\",\"type\":\"string\",\"doc\":\"First Name of the Customer\"},\n" +
                "        {\"name\":\"last_name\",\"type\":\"string\",\"doc\":\"Last Name of the Customer\"},\n" +
                "        {\"name\":\"age\",\"type\":\"int\",\"doc\":\"Age of the Customer\"},\n" +
                "        {\"name\":\"height\",\"type\":\"float\",\"doc\":\"Height in cms\"},\n" +
                "        {\"name\":\"weight\",\"type\":\"float\",\"doc\":\"Weight in kgs\"},\n" +
                "        {\"name\":\"automated_email\",\"type\":\"boolean\", \"default\":true, \"doc\":\"true if user wants marketing emails\"}\n" +
                "    ]\n" +
                "}");

        //step 1: Create generic record
        GenericRecordBuilder customerbuilder = new GenericRecordBuilder(schema);
        customerbuilder.set("first_name","John");
        customerbuilder.set("last_name","Doe");
        customerbuilder.set("age",25);
        customerbuilder.set("height",170f);
        customerbuilder.set("weight",80.5f);
        customerbuilder.set("automated_email",false);
        Record customer = customerbuilder.build();

        LOGGER.info(customer.toString());

        //step 2: Write generic record to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            LOGGER.info("Written customer-generic.avro");
        } catch (IOException e) {
            LOGGER.info("Couldn't write file");
            e.printStackTrace();
        }

        //step 3: Read generic record from a file
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){
            customerRead = dataFileReader.next();

            //step 4: Interpret as a generic record
            LOGGER.info("Successfully read avro file");
            LOGGER.info(customerRead.toString());

            // get the data from the generic record
            LOGGER.info("First name: {}", customerRead.get("first_name"));

            // read a non existent field
            LOGGER.info("Non existent field: {}", customerRead.get("not_here"));
        }
        catch(IOException e) {
            e.printStackTrace();
        }



    }
}
