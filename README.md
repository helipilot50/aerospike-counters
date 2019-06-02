# Atomic Counters using Aerospike (WORK IN PROGRESS)
How to create atomic counters using [Aerospike](www.aerospike.com%)

# Companion Code
The companion code is deliberately simple to focus on counters without the distraction of idiomatic language patterns or frameworks. Examples are provided in Java and C#.

Clone the repository using:
```bash
git clone https://github.com/helipilot50/aerospike-counters.git
```

# Local aerospike datbase using docker
```bash
sudo docker-compose -f aerospike-single-docker-compose.yml up
```
# Run Dotnet example
```bash
dotnet build
dotnet run
```

# Run Java example
```bash
mvn build
mvn exec:java
```
