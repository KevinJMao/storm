package com.kevinmao;

import java.io.File;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.nio.file.FileSystems;
import java.util.Properties;
import java.io.*;
import java.util.Scanner;


public class TextKafkaProducer {
    private static final String RESOURCE_DIRECTORY_PATH = "";
    private static final String TXT_EXTENTION = "txt";

    private String txt_file_name;
    private String kafkaOutputTopic;
    private File directory;
    private File[] directoryListing;
    private Producer<String, String> producer;

    public TextKafkaProducer(String inputFile, String outputTopic)
    {
        directory = new File(RESOURCE_DIRECTORY_PATH);
        directoryListing = directory.listFiles();
        txt_file_name = inputFile;
        kafkaOutputTopic = outputTopic;
        producer = new Producer<String, String>(initializeProperties());
    }

    public void retrievePcapLines() {
        System.out.println("Input file:" + txt_file_name);
        System.out.println("Output topic:" + kafkaOutputTopic);

        File f = new File(txt_file_name);

        StringBuilder errbuff = new StringBuilder();

        assert(f.exists());
        assert(f.isFile());
        assert(f.canRead());
        assert(f.getAbsolutePath().endsWith(TXT_EXTENTION));

        if(f.isFile() && f.canRead() && f.getAbsolutePath().endsWith(TXT_EXTENTION)) {
            System.out.println("Opening " + f.getPath());

            try {
                Scanner in = new Scanner(new FileReader(f));

                while (in.hasNextLine()) {
                    String record = in.nextLine().trim();
                    String[] recordSplit = record.split("[\\s]");
                    String recordIndex = record.split("[\\s]")[0];
                    assert(Integer.parseInt(recordIndex) >= 0);
//                    producer.send(new KeyedMessage<String,String>(kafkaOutputTopic, recordIndex, record));
                    System.out.println(recordIndex + " " + record);
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println(e.getStackTrace()[0]);
            }
        }
    }

    private ProducerConfig initializeProperties() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "kafka1.kevinmao.com:9092,kafka2.kevinmao.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        return config;
    }

    public void printFileList() {
        for(File file : directoryListing) {
            System.out.println(file.getPath());
        }
    }

    public static void main(String[] args) {
        assert(System.getProperty("input.file") != null);
        assert(System.getProperty("output.topic") != null);
        System.out.println("Java class path: " + System.getProperty("java.class.path"));
        System.out.println("User directory: " + System.getProperty("user.dir"));
        System.out.println("input.file: " + System.getProperty("input.file"));
        System.out.println("output.topic: " + System.getProperty("output.topic"));

        TextKafkaProducer producer = new TextKafkaProducer(System.getProperty("input.file"),
                System.getProperty("output.topic"));
        producer.retrievePcapLines();
    }
}
