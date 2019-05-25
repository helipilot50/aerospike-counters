package net.helipilot50.atomiccounter;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Operation;

/**
 * Hello world!
 *
 */
public class SingleAtomicCounter {
    public static void main(String[] args) {
        System.out.println("Hello World!");
    }

    public AerospikeClient asClient;
    

    public AtomicCounter() {
        this.asClient = new AerospikeClient("localhost", 3000);
    }

    /**
     * Increments a counter and returns the new value
     * @param counterName The record key for the counter
     * @param by The value to increment, or decrement, the counter
     * @return the new value of the counter
     */
    public long incrementSingle(String counterName, long by){

        // Create a key
        Key recordKey = new Key(Constants.NAMESPACE, Constants.SINGLE_SET, counterName);

        // Increment operation
        Bin incrementCounter = new Bin(Constants.COUNTER_BIN, by);

        // https://www.aerospike.com/docs/client/java/usage/kvs/multiops.html#operation-specification
        Record record = asClient.operate(null, recordKey, Operation.add(incrementCounter), Operation.get(Constants.COUNTER_BIN));

        return record.getLong(Constants.COUNTER_BIN);
    }
    /**
     * Increment, or decrement, atomically two counters in the same record
     * @param counterName the key for the counter record
     * @param firstCounter First counter name
     * @param firstBy Increment or decremt first counter by
     * @param secondCounter Second counter name
     * @param secondBy Increment or decremt second counter by
     * @return
     */
    public LongTuple incrementMultiple(String counterName, String firstCounter, long firstBy, String secondCounter, long secondBy){

        // Create a key
        Key recordKey = new Key(Constants.NAMESPACE, Constants.MULTI_SET, counterName);

        // Increment operations
        Bin incrementCounter1 = new Bin(firstCounter, firstBy);
        Bin incrementCounter2 = new Bin(secondCounter, secondBy);

        // https://www.aerospike.com/docs/client/java/usage/kvs/multiops.html#operation-specification
        Record record = asClient.operate(null, recordKey, Operation.add(incrementCounter1), Operation.add(incrementCounter2), Operation.get(firstCounter), Operation.get(firstCounter));
        return new LongTuple(record.getLong(firstCounter), record.getLong(secondCounter));
    }

    protected void finalize() {
        asClient.close();
        asClient = null;
    }

    /**
     * simple tuple to hold two longs
     */
    public class LongTuple {
        public long left;
        public long right;
        public LongTuple(long left, long right){
            this.left = left;
            this.rignt = rignt;
        }
    }
}
