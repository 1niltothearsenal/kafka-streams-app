package com.gooners.avro;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExamples {

    public static final Logger LOGGER = LoggerFactory.getLogger(SpecificRecordExamples.class);

    public static void main(String[] args) {

        //step 1: create specific record
        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Doe");
        customerBuilder.setHeight(175.5f);
        customerBuilder.setWeight(80.5f);
        customerBuilder.setAutomatedEmail(false);
        Customer customer = customerBuilder.build();

        LOGGER.info(customer.toString());

        //step 2: write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            LOGGER.info("Successfully wrote customer-specific.avro");
        } catch (IOException e) {
            LOGGER.info("Couldn't write file");
            e.printStackTrace();
        }

        //step 3: read from file
        final File file = new File("customer-generic.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        final DataFileReader<Customer> dataFileReader;
        try {
            LOGGER.info("Reading our specific record");
            dataFileReader = new DataFileReader<>(file,datumReader);
            while (dataFileReader.hasNext()){
                Customer readCustomer = dataFileReader.next();
                LOGGER.info(readCustomer.toString());
                LOGGER.info("First Name: " + readCustomer.getFirstName());
            }
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        //step 4: interpret
    }
}
