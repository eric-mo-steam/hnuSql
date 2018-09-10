package storage.example;

import structure.Record;
import structure.Factory;
import structure.Schema;

public class FactoryImpl implements Factory {

    @Override
    public Record[] produceRecords(int recordSize, int columnSize) {
        Record[] records = new Record[recordSize];
        for (int i = 0;i < recordSize;i++) {
            records[i] = new RecordImpl(columnSize);
        }
        return records;
    }

    @Override
    public Schema produceSchema() {
        return new SchemaImpl();
    }
}
