package storage.example;

import structure.Record;
import structure.Factory;
import structure.Schema;

public class FactoryImpl implements Factory {

    @Override
    public Record[] produceRecords(int size) {
        Record[] records = new Record[size];
        for (int i = 0;i < size;i++) {
            records[i] = new RecordImpl();
        }
        return records;
    }

    @Override
    public Schema produceSchema() {
        return new SchemaImpl();
    }
}
