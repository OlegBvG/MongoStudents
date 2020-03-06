import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


public class Main {

    private static final Logger logger = LogManager.getLogger();
    private static final int SLEEP = 1000;

    public static void main(String[] args) {

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

        Runtime r = Runtime.getRuntime();
        Process p = null;

        String command = "d:\\Dev\\mongodb\\bin\\mongoimport --db test --collection Students -f Name,Age,Courses --type csv --file d:\\Skill\\IdeaProjects\\MongoStudents\\src\\main\\resources\\mongo.csv";

        try {
            p = r.exec(command);
            logger.info("Reading csv into Database");
            Thread.sleep(SLEEP);

        } catch (Exception e){

            logger.info("Error executing " + command + e.toString());
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

