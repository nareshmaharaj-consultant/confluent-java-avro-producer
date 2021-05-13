package example.avro;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class CreateUsers {
    public static void main(String[] args) {

        System.out.println("-- Using Avro Source Files for User Doc --");
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        // Leave favorite color null

        // Alternate constructor
        User user2 = new User("Ben", 7, "red");

        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        try {
            dataFileWriter.create(user1.getSchema(), new File("users.avro"));
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Deserialize Users from disk
        try {
            String fileIn = "users.avro";
            File file = new File(fileIn);
            DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
            DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
            User user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException io) {
            io.printStackTrace();
        }

        /*
            Data in Avro is always stored with its corresponding schema,
            meaning we can always read a serialized item regardless of
            whether we know the schema ahead of time.
            This allows us to perform serialization and deserialization
            without code generation.
         */
        System.out.println("\n-- Using Generic Parser for User Doc --");

        try {
            Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));
            GenericRecord userG1 = new GenericData.Record(schema);
            userG1.put("name", "Alyssa");
            userG1.put("favorite_number", 256);
            // Leave favorite color null

            GenericRecord userG2 = new GenericData.Record(schema);
            userG2.put("name", "Ben");
            userG2.put("favorite_number", 7);
            userG2.put("favorite_color", "red");

            // Serialize user1 and user2 to disk
            File file = new File("users_generic.avro");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            DataFileWriter<GenericRecord> dataFileWriterG = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriterG.create(schema, file);
            dataFileWriterG.append(user1);
            dataFileWriterG.append(user2);
            dataFileWriterG.close();


            // Deserialize users from disk
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
