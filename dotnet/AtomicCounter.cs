using System;
using Aerospike.Client;

namespace dotnet
{

    class AtomicCounter
    {
        AerospikeClient asClient;
        public const string NAMESPACE = "test";
        public const string SINGLE_SET = "single-counter";
        public const string MULTI_SET = "multi-counter";
        public const string SINGLE_COUNTER_BIN = "counter-bin";

        static void Main(string[] args)
        {
            Console.WriteLine("Aerospike counters");
            AtomicCounter atomic = new AtomicCounter();
            long newValue = atomic.IncrementSingle("a-single-counter", 1);
            Console.WriteLine("- single Atomic value {0}", newValue);
            Tuple<Int64, Int64> newValues = atomic.IncrementMultiple("multiple-counters", "first-counter", 1L, "second-counter", 1L);
            Console.WriteLine("- single Atomic value {0}, {0}", newValues.Item1, newValues.Item2);
        }

        public AtomicCounter()
        {
            this.asClient = new AerospikeClient("localhost", 3000);
        }
        public long IncrementSingle(string counterName, long by)
        {

            // Create a key
            Key recordKey = new Key(NAMESPACE, SINGLE_SET, counterName);

            // Increment operation
            Bin incrementCounter = new Bin(SINGLE_COUNTER_BIN, by);

            // https://www.aerospike.com/docs/client/java/usage/kvs/multiops.html#operation-specification
            Record record = asClient.Operate(null, recordKey, Operation.Add(incrementCounter), Operation.Get(SINGLE_COUNTER_BIN));

            return record.GetLong(SINGLE_COUNTER_BIN);
        }

        public Tuple<Int64, Int64> IncrementMultiple(string counterName, string firstCounter, long firstBy, string secondCounter, long secondBy)
        {

            // Create a key
            Key recordKey = new Key(NAMESPACE, MULTI_SET, counterName);

            // Increment operations
            Bin incrementCounter1 = new Bin(firstCounter, firstBy);
            Bin incrementCounter2 = new Bin(secondCounter, secondBy);

            // https://www.aerospike.com/docs/client/java/usage/kvs/multiops.html#operation-specification
            Record record = asClient.Operate(null, recordKey, Operation.Add(incrementCounter1), Operation.Add(incrementCounter2), Operation.Get(firstCounter), Operation.Get(firstCounter));
            return Tuple.Create<Int64, Int64>(record.GetLong(firstCounter), record.GetLong(secondCounter));
        }
    }
}
