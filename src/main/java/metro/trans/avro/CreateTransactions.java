package metro.trans.avro;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class CreateTransactions {
    public static void main(String[] args) {
        System.out.println("-- Using Avro Source Files for Transaction Doc --");

        BusinessKey businessKey = new BusinessKey();
        businessKey.setTxId("1");

        MetaData md = new MetaData();
        md.setMsgType("PAC0008");
        md.setMdInsertDtTm("2021-05-12T14:59:27");

        Details d = new Details();
        d.setDate("2021-05-12T15:59:27.117Z");
        d.setAmount(34.68);

        Transactions t = new Transactions();
        t.setAccountId("IBAN0000000031673629402517");
        t.setTranactionId(businessKey.getTxId());
        t.setDataPayLoad(d);
        t.setBusinessKey(businessKey);
        t.setMetadata(md);

        DatumWriter<Transactions> transactionDatumWriter = new SpecificDatumWriter<Transactions>(Transactions.class);
        DataFileWriter<Transactions> dataFileWriter = new DataFileWriter<Transactions>(transactionDatumWriter);
        try {
            dataFileWriter.create(t.getSchema(), new File("transactions.avro"));
            dataFileWriter.append(t);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Deserialize Users from disk
        try {
            String fileIn = "transactions.avro";
            File file = new File(fileIn);
            DatumReader<Transactions> transactionsDatumReader = new SpecificDatumReader<Transactions>(Transactions.class);
            DataFileReader<Transactions> dataFileReader = new DataFileReader<Transactions>(file, transactionsDatumReader);
            Transactions tx = null;
            while (dataFileReader.hasNext()) {
                tx = dataFileReader.next(tx);
                System.out.println(tx);
            }
        } catch (IOException io) {
            io.printStackTrace();
        }
    }


}
