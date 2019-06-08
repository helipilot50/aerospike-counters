import aerospike from 'Aerospike';


const NAMESPACE = "test";
const SINGLE_SET = "single-counter";
const MULTI_SET = "multi-counter";
const SINGLE_COUNTER_BIN = "counter-bin";
const RECORD_SET = "record-with-counter";
const NAME_BIN = "name-bin";
const VISIT_BIN = "visit-bin";


class AtomicCounter {


    constructor(asClient) {
        this.asClient = asClient;
    }

    async incrementSingle(counterName, by) {

        const recordKey = new aerospike.Key(NAMESPACE, SINGLE_SET, counterName);

        const ops = [
            aerospike.operations.incr(SINGLE_COUNTER_BIN, by),
            aerospike.operations.read(SINGLE_COUNTER_BIN)
        ]

        // https://www.aerospike.com/docs/client/nodejs/usage/kvs/multiops.html
        const record = await this.asClient.operate(recordKey, ops);

        return record.bins[SINGLE_COUNTER_BIN];
    }

    async incrementMultiple(counterName, firstCounter, firstBy, secondCounter, secondBy) {

        const recordKey = new aerospike.Key(NAMESPACE, MULTI_SET, counterName);

        const ops = [
            aerospike.operations.incr(firstCounter, firstBy),
            aerospike.operations.incr(secondCounter, secondBy),
            aerospike.operations.read(firstCounter),
            aerospike.operations.read(secondCounter)
        ]

        // https://www.aerospike.com/docs/client/nodejs/usage/kvs/multiops.html
        const record = await this.asClient.operate(recordKey, ops);
        return [record.bins[firstCounter], record.bins[secondCounter]];
    }
    async incrementVisits(userId, name) {

        const recordKey = new aerospike.Key(NAMESPACE, RECORD_SET, userId);

        const ops = [
            aerospike.operations.write(NAME_BIN, name),
            aerospike.operations.incr(VISIT_BIN, 1),
            aerospike.operations.read(NAME_BIN),
            aerospike.operations.read(VISIT_BIN)
        ]

        // https://www.aerospike.com/docs/client/nodejs/usage/kvs/multiops.html
        const record = await this.asClient.operate(recordKey, ops);
        return { userId: userId, name: record.bins[NAME_BIN], visitCount: record.bins[VISIT_BIN] };
    }

}


let main = async () => {
    try {
        let client = aerospike.client({
            hosts: [
                { addr: "127.0.0.1", port: 3000 }
            ],
            log: {
                level: aerospike.log.INFO
            }
        });
        let asClient = await client.connect();

        console.info("Aerospike counters");
        const atomic = new AtomicCounter(asClient);
        let newValue = await atomic.incrementSingle("a-single-counter", 1);
        console.info("- single Atomic value ", newValue);
        let newValues = await atomic.incrementMultiple("multiple-counters", "first-counter", 7, "second-counter", 1);
        console.info("- Two Atomic values ", newValues);
        let user = await atomic.incrementVisits("helipilot50", "Peter Milne");
        console.info("- User: ", user);


    } catch (error) {
        console.error("Error: ", error);
    }
    process.exit(0);
}
main();