noworlater
----------

This is a prototype of a time-based priority queue that uses Redis soorted
sets and a Kinesis-based write ahead log. Who knows if it's actually a good
idea to do it this way. 

If you want to play with it, start up a local redis and Kinesis instance
by running docker-compose up. Use the kinesis API through the sbt console
to create a Kinesis stream called "test" and run 
```
export AWS_CBOR_DISABLE=true # Needed to kinesislite to work with the SDK
sbt assembly && java -classpath target/scala-2.12/noworlater-assembly-1.0.jar org.brett.noworlater.Worker
````

This starts up a Worker process that consumes the Kinesis stream, waiting for
"Add" messages. Add messages contain one or more tuples of (unique id, delivery ms).
These messages get added to a redis sorted set. After the worker has caught up
with the stream, it will poll redis for any messages with delivery ms < the current time
and writes "Remove" messages for those records on the stream. When the worker encounters a "Remove"
message, it removes element from the sorted set.

Consumers of delayed messages simply need to consume the stream and take
action when they encounter a relevant "Remove" message.

There are a few test classes you can play with:

- org.brett.noworlater.Consumer - Watches the stream and logs a message to
stdout whenever it ecounters a Remove message.
- org.brett.noworlater.Producer - Reads lines from stdin in the form "<id> <delay>"
and enqueues Add messages onto the stream with the given delay from the current time
- org.brett.noworlater.TimestampLoadTest - Reads numeric lines from stdin that represent
a ms offset from the current time and enqueues messages at a steady rate with their ISO timestamp
as the id. You can use this to test the behavior of the system with different distributions
of data.

