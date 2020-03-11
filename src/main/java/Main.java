import au.com.bytecode.opencsv.CSVReader;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Main {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws IOException {

        logger.info("Log4j2ExampleApp started.");
        logger.warn("Something to warn");
        logger.error("Something failed.");
        try {
            Files.readAllBytes(Paths.get("/file/does/not/exist"));
        } catch (IOException ioex) {
            logger.error("Error message", ioex);
        }

        MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );

        MongoDatabase database = mongoClient.getDatabase("test");

       // Создаем коллекцию
        MongoCollection<Document> collection = database.getCollection("Students");

        // Удалим из нее все документы
        collection.drop();


        String mongoCsvFile = Main.class.getResource("mongo.csv").getFile();
        CSVReader reader = null;
        try
        {
            //Get the CSVReader instance with specifying the delimiter to be used
            reader = new CSVReader(new FileReader(mongoCsvFile),',');
            String [] nextLine;

            //Read one line at a time
            while ((nextLine = reader.readNext()) != null)
            {
                collection.insertOne(new Document()
                        .append("Name", nextLine[0])
                        .append("Age",  Integer.parseInt(nextLine[1]))
                        .append("Courses", nextLine[2]));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

             for (String name : database.listCollectionNames()) {
                logger.info("Коллекция - " + name);
            }

        // Get MongoDb Collections Count
        logger.info("\nОбщее количество студентов в базе ===>  "+ collection.countDocuments() + " человек.");

        logger.info("\nИз них старше 40 лет:");

        var query = new BasicDBObject("Age",
                new BasicDBObject("$gt", 40));

        AtomicInteger countStudents = new AtomicInteger();
        collection.find(query).forEach((Consumer<Document>) doc -> {
            logger.info(doc.toJson());
            countStudents.getAndIncrement();
        });
        logger.info("--- всего: " + countStudents + " студентов.");

        FindIterable fit = collection.find().sort(new Document("Age", 1)).limit(1);

        var docs = new ArrayList<Document>();

        fit.into(docs);

        for (Document doc : docs) {

            logger.info("\nСамый молодой студент ===>  " + doc.getString("Name") + "  возраст: " + doc.getInteger("Age") + " лет.");
        }

         fit = collection.find().sort(new Document("Age", -1)).limit(1);

         docs = new ArrayList<Document>();

        fit.into(docs);

        for (Document doc : docs) {

            logger.info("\nСамый старший студент " + doc.getString("Name") + "  изучает: " + doc.getString("Courses") + "\n");
        }
    }
}

