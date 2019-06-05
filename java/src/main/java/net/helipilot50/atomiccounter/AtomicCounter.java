package net.helipilot50.atomiccounter;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Operation;


/**
 * Acomic Counters in Aerospike
 *
 */
public class AtomicCounter {
    public final String NAMESPACE = "test";
    public final String SINGLE_SET = "single-counter";
    public final String MULTI_SET = "multi-counter";
    public final String RECORD_SET = "record-with-counter";
    public final String NAME_BIN = "name-bin";
    public final String VISIT_BIN = "visit-bin";
    public final String SINGLE_COUNTER_BIN = "counter-bin";

    public static void main(String[] args) {
        System.out.println("Hello World!");
        System.out.println("Aerospike counters");
        AtomicCounter atomic = new AtomicCounter();
        long newValue = atomic.incrementSingle("a-single-counter", 1);
        System.out.println(String.format("- One Atomic value %d", newValue));
        LongTuple newValues = atomic.incrementMultiple("multiple-counters", "first-counter", 1L, "second-counter", 7L);
        System.out.println(String.format("- Two Atomic values %d, %d", newValues.left, newValues.right));
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
        Bin incrementCounter = new Bin(Constants.SINGLE_COUNTER_BIN, by);

        // https://www.aerospike.com/docs/client/java/usage/kvs/multiops.html#operation-specification
        Record record = asClient.operate(null, recordKey, 
                            Operation.add(incrementCounter), 
                            Operation.get(Constants.SINGLE_COUNTER_BIN));

        return record.getLong(Constants.SINGLE_COUNTER_BIN);
    }
    /**
     * Increment, or decrement, atomically two counters in the same record
     * @param counterName the key for the counter recordd
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
        Record record = asClient.operate(null, recordKey, 
                                Operation.add(incrementCounter1), 
                                Operation.add(incrementCounter2), 
                                Operation.get(firstCounter), 
                                Operation.get(secondCounter));
        return new LongTuple(record.getLong(firstCounter), record.getLong(secondCounter));
    }

    public User incrementVisits(String userId, String name){

        // Create a key
        Key recordKey = new Key(Constants.NAMESPACE, Constants.RECORD_SET, userId);

        // Increment operations
        Bin nameBin = new Bin(Constants.NAME_BIN, name);
        Bin visitBin = new Bin(Constants.VISIT_BIN, 1);

        // https://www.aerospike.com/docs/client/java/usage/kvs/multiops.html#operation-specification
        Record record = asClient.operate(null, recordKey, 
                                Operation.add(visitBin), 
                                Operation.put(nameBin), 
                                Operation.get(Constants.NAME_BIN), 
                                Operation.get(Constants.VISIT_BIN));
        return new User(userId, record.getString(Constants.NAME_BIN), record.getLong(Constants.VISIT_BIN));
    }
    /**
     * close the Aerospike client on process termination
     */
    protected void finalize() {
        this.asClient.close();
        this.asClient = null;
    }

    /**
     * simple tuple to hold two longs
     */
    public class LongTuple {
        public long left;
        public long right;
        public LongTuple(long left, long right){
            this.left = left;
            this.right = right;
        }
    }

    public class User {
        String id;
        String name;
        Long visitCount;
    
        public User(String id, String name, Long visitCount){
            this.name = name;
            this.id = id;
            this.visitCount = visitCount;
        }
    
        @Override
        public String toString() {
            return String.format("{id: %s, name: %s, visitCount: %d}", this.id, this.name, this.visitCount);
        }
    
    }
}
