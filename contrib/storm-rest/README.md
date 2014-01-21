# Storm-Rest
Simple web service that exposes Storm cluster metrics in JSON format.

## Building from Source
```
mvn clean install
```

## Running
Copy `config_example.yaml` and change the `nimbusHost` parameter to point to the Storm nimbus node. Optionally
change the service/admin ports


    nimbusHost: "nimbus"
    nimbusPort: 6627

    # HTTP-specific options.
    http:

      # The port on which the HTTP server listens for service requests.
      port: 8080

      # The port on which the HTTP server listens for administrative requests.
      adminPort: 8081


Start the rest service:

    java -jar target/storm-rest-1.0-SNAPSHOT.jar server ./config.yaml


## REST Endpoints

    GET     /api/cluster/configuration
    GET     /api/cluster/summary
    GET     /api/supervisors/summary
    GET     /api/topology/summary


## Sample Usage

    $ curl -X GET http://localhost:8080/api/topology/summary
    [ {
      "id" : "word-count-1-1389887079",
      "tasks.total" : 28,
      "executors.total" : 28,
      "status" : "ACTIVE",
      "name" : "word-count",
      "uptime" : 3485,
      "workers.total" : 3
    } ]
