package com.cookpad.prism.record;

import java.util.List;

import org.apache.parquet.io.api.RecordConsumer;
import com.cookpad.prism.record.values.Value;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
@EqualsAndHashCode
public class ValueListRecord implements Record {
    final private List<Value> values;

    public void writeMessage(RecordConsumer consumer) {
        consumer.startMessage();
        for (Value value: this.values) {
            value.writeField(consumer);
        }
        consumer.endMessage();
    }

    @Override
    public Value getValue(int index) {
        return this.values.get(index);
    }
}
