# Atomic Counters using Aerospike
How to create atomic counters using [Aerospike](www.aerospike.com)
Read the full article at [Medium](https://link.medium.com/xo6luzGLUX)
# Companion Code
The companion code is deliberately simple to focus on counters without the distraction of idiomatic language patterns or frameworks. Examples are provided in Java, C# and JavaScript ES6.

Clone the repository using:
```bash
git clone https://github.com/helipilot50/aerospike-counters.git
```

# Local aerospike database using docker
```bash
sudo docker-compose -f aerospike-single-docker-compose.yml up
```
# Run C# example
```bash
dotnet build
dotnet run
```

# Run Java example
```bash
mvn build
mvn exec:java
```
# Run JavaScript/Node example
```bash
npm install
npm start
```
