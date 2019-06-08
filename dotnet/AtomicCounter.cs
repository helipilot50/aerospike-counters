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
        public const string RECORD_SET = "record-with-counter";
        public const string NAME_BIN = "name-bin";
        public const string VISIT_BIN = "visit-bin";

        static void Main(string[] args)
        {
            Console.WriteLine("Aerospike counters");
            AtomicCounter atomic = new AtomicCounter();
            long newValue = atomic.IncrementSingle("a-single-counter", 1);
            Console.WriteLine("- single Atomic value {0}", newValue);
            Tuple<Int64, Int64> newValues = atomic.IncrementMultiple("multiple-counters", "first-counter", 7L, "second-counter", 1L);
            Console.WriteLine("- Two Atomic values {0}, {1}", newValues.Item1, newValues.Item2);
            User user = atomic.IncrementVisits("helipilot50", "Peter Milne");
            Console.WriteLine("- User: {0}", user.ToString());
        }

        public AtomicCounter()
        {
            this.asClient = new AerospikeClient("localhost", 3000);
        }
        public long IncrementSingle(string counterName, long by)
        {

            Key recordKey = new Key(NAMESPACE, SINGLE_SET, counterName);

            Bin incrementCounter = new Bin(SINGLE_COUNTER_BIN, by);

            Record record = asClient.Operate(null, recordKey, Operation.Add(incrementCounter), Operation.Get(SINGLE_COUNTER_BIN));

            return record.GetLong(SINGLE_COUNTER_BIN);
        }

        public Tuple<Int64, Int64> IncrementMultiple(string counterName, string firstCounter, long firstBy, string secondCounter, long secondBy)
        {

            Key recordKey = new Key(NAMESPACE, MULTI_SET, counterName);

            Bin incrementCounter1 = new Bin(firstCounter, firstBy);
            Bin incrementCounter2 = new Bin(secondCounter, secondBy);

            Record record = asClient.Operate(null, recordKey, Operation.Add(incrementCounter1), Operation.Add(incrementCounter2), Operation.Get(firstCounter), Operation.Get(secondCounter));
            return Tuple.Create<Int64, Int64>(record.GetLong(firstCounter), record.GetLong(secondCounter));
        }
        public User IncrementVisits(String userId, String name)
        {

            Key recordKey = new Key(NAMESPACE, RECORD_SET, userId);

            Bin nameBin = new Bin(NAME_BIN, name);
            Bin visitBin = new Bin(VISIT_BIN, 1);

            Record record = asClient.Operate(null, recordKey,
                                    Operation.Add(visitBin),
                                    Operation.Put(nameBin),
                                    Operation.Get(NAME_BIN),
                                    Operation.Get(VISIT_BIN));
            return new User(userId, record.GetString(NAME_BIN), record.GetLong(VISIT_BIN));
        }

        ~AtomicCounter()
        {
            this.asClient.Close();
        }
    }

    class User
    {
        private string id;
        private string name;
        private long visitCount;

        public User(String id, String name, long visitCount)
        {
            this.name = name;
            this.id = id;
            this.visitCount = visitCount;
        }

        public override String ToString()
        {
            return String.Format("id: {0}, name: {1}, visitCount: {2}", this.id, this.name, this.visitCount);
        }

    }

}
