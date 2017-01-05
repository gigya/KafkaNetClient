KafkaNetClient
=========

Native C# client for Apache Kafka with full async support.

Kafka versions [0.8, 0.10.1]

License
-----------
Copyright 2016, Nudge Software Inc under Apache License, V2.0. See LICENSE file.

History
-----------
This library is a fork of [gigya]'s [KafkaNetClient], itself a fork of [jroland]'s [kafka-net] library. There are significant API changes.

The original .Net project is a port of the [Apache Kafka protocol]. The wire protocol portion is based on the [kafka-python] library writen by [David Arthur] and the general class layout attempts to follow a similar pattern as his project. To that end, this project builds up from the low level Connection object for handling async requests to/from the kafka server, all the way up to a higher level Producer/Consumer classes.

Code Examples
-----------
##### Producer
```csharp
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));

using(var client = new Producer(options)) {
     await client.SendMessageAsync("TestTopic", new Message("hello world"));
}
```

##### Consumer
```csharp
var options = new KafkaOptions(new Uri("http://SERVER1:9092"), new Uri("http://SERVER2:9092"));

using (var router = new BrokerRouter(options)) {
	var offset = await router.GetTopicOffsetAsync("TestTopic", 0, CancellationToken.None);

	using (var consumer = new Consumer(router)) {
	    await consumer.FetchAsync(offset, 10, message => Console.WriteLine("Response: P{0},O{1} : {2}", message.Meta.PartitionId, message.Meta.Offset, message.Value), cancellationToken);
	}
}
```

Key Components
-----------
##### Connection (`KafkaClient.Connection.IConnection`)
Provides async methods on a persistent connection to a kafka broker (server).  The send method uses the TcpClient sendAsync function and the read stream has a dedicated thread which uses the correlation Id to match send responses to the correct request.

##### Producer
Provides a high level abstraction for sending batches of messages to a Kafka cluster. Internally, it uses the IBrokerRouter and IConnection.

##### Consumer
Provides a mechanism for fetching messages from a Kafka cluster.

##### Consumer Assignment (`KafkaClient.Assignment`)
Provides an extesible mechanism for assigning partitions to members of consumer groups, enabling complex coordination across multiple consumers of a set of Kafka topics. The default assignor (ConsumerAssignor) will round robin partition selection across topics.

##### Partition Selection (`KafkaClient.IPartitionSelector`)
Provides the logic for routing requests to a particular topic to a partition. The default selector (PartitionSelector) will use round robin partition selection if the key property on the message is null and a mod/hash of the key value if present.

##### BrokerRouter (`KafkaClient.IBrokerRouter`)
Provides routing for topics to partitions, topics and partitions to brokers, and cached access to topic metadata. This class also manages the multiple IConnections to each Kafka broker. Routing logic for partitions is provided by IPartitionSelector.

##### Binary Protocol (`KafkaClient.Protocol.*`)
The protocol has been divided up into concrete classes for each request/response pair. An Encoder class knows how to encode requests and decode responses into/from their appropriate Kafka protocol byte array. One benefit of this is that it allows for a nice generic send method on the Connection.

Status
-----------
[![Build status](https://ci.appveyor.com/api/projects/status/e7ej2g9q77if8mkf/branch/master?svg=true)](https://ci.appveyor.com/project/AndrewRobinson/kafkanetclient/branch/master)

** WARNING **
This library is still work in progress and has not yet been deployed to production. It is also undergoing significant development, and breaking changes will occour.
This notice will be removed once it's been stabilized and used in production.

The biggest missing piece at this point is [stress testing](https://github.com/awr/KafkaClient/issues/17); more [comprehensive automated tests](https://github.com/awr/KafkaClient/issues/18) are also needed. For the full set, see the [backlog]
(https://github.com/awr/KafkaClient/projects/1).

[Apache Kafka protocol]:https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
[kafka-python]:https://github.com/mumrah/kafka-python
[David Arthur]:https://github.com/mumrah
[kafka-net]:https://github.com/Jroland/kafka-net
[jroland]:https://github.com/jroland
[KafkaNetClient]:https://github.com/gigya/KafkaNetClient
[gigya]:https://github.com/gigya
