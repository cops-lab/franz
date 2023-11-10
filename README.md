# Franz, the opinionated Kafka library

This repository contains *Franz*, the opinionated Kafka library.
It is implemented as a layer on top of the regular `kafka-client` library and simplifies the interaction with messages and topics.
The goal of *Franz* is to provide a simple, type-safe publish/subscribe interface that keeps most of the configuration adjustable, while reducing the complexity and intricacies of Kafka to a minimum.

The *Franz* repository has three main parts: The `api` packages contains the API that is required to work with *Franz*. The `impl` package contains a reference implementation of the API. The `examples` folder contains a basic set of examples that illustrate the library usage.

*Franz* provides the following benefits over the basic `kafka-client` library:

- Kafka's inherent complexity is reduced to its basic publish/subscribe paradigm.
- Easy-to-use interface with a functional programming style.
- Built-in support for prioritization and error handling.
- Type safe producing and consuming of messages.
- No error-prone construction/parsing of JSON.
- Incremental subscriptions to topics.

**Please note:** Packages of *Franz* are released in the [COPS Lab Packages Repository](https://github.com/cops-lab/packages) and can be added to your project as a regular Maven dependency. Refer to the GitHub documentation to understand [how to add that repository as a package registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry) to a Maven `pom.xml` file.

## Initialization

The *Franz* API is an abstraction of the Kafka client and mainly consists of two components: the interface `Kafka` and a `KafkaException` for error handling.
An implementation of `Kafka` is provided through `KafkaImpl`, which requires three parameters for its initialization:

- Instance of `KafkaConnector`, which can be easily instantiated by providing the *url* to a Kafka server and the *group id* that should be used for the *consumer group* (Please refer to the [Kafka developer resources](https://developer.confluent.io/) to learn more about fundamental concepts).
- Instance of `JsonUtils`, which in turn provides a layer on top of Jacksons `ObjectMapper`. A basic instantiation is as easy as `new JsonUtils(new ObjectMapper())`, but you can find an extended setup with custom data structures and (de-)serializers in the [Main class](examples/src/main/java/dev/c0ps/franz/Main.java) of the examples.
- Defining whether the Kafka library should `shouldAutoCommit` consumed messages. A sensible default is `true`, which means that every *completed* `poll` will mark the message as consumed. Setting the parameter to `false` requires a programmatic call of `commit`.

It is recommended to use `Kafka`/`KafkaImpl` with dependency injection (e.g., [Guice](https://github.com/google/guice), [Spring](https://spring.io/)).
In such an environment, bind `Kafka` to `KafkaImpl` and request an injection of `Kafka` in dependent components.

## Usage (Basic)

The main concepts that are important to understand in the *Franz* library are the ideas of a `Lane`, of *base topics*, and of *combined topics*.
When interacting with the API, users need to refer to a *base topic* (e.g., `mytopic`) and a `Lane` (e.g., `PRIORITY`).
*Franz* will merge this into a *combined topic* that concatenates both (e.g., `mytopic-PRIORITY`).
The *base topic* is only relevant for the abstraction within *Franz*, from the perspective of the Kafka server, only the *combined topic* will be used.

The abstraction of *Franz* enables the use of a prioritization system for messages (while using a fixed set of consumers).
To achieve this, every instance of `KafkaImpl` will start two consumers, which are responsible for registering for *every* subscribed base topic on their corresponding lane (i.e., the combined topics `mytopic-NORMAL` and `mytopic-PRIORITY`).
When new messages arrive, `KafkaImpl` will dispatch the messages to the subscribed callbacks.
The prioritization is then achieved by always giving precendence to messages on the `PRIORITY` lane over messages on the `NORMAL` lane.
Usually, polling does not introduce any waiting delay.
*Only if no message exist on either lane*, polling will wait for new messages on the `PRIORITY` lane and only check the `NORMAL` lane every couple of seconds.

To `publish` a message, it is required to specify the target `Lane`.
While `publish` can be called in isolation (and `Lane` can be freely chosen), in many cases, `publish` is called from within a subscribed callback.
In this case, the `Lane` parameter can just be taken from the callback arguments to make the outgoing message stay in the same `Lane` as the incoming message.

In the following, all methods are introduced that are required to get started with using the API.
All examples assume that a `Kafka`/`KafkaImpl` instance is available in variable `k`.
A more complete example of how to use this library in practice is given in the [usage example](examples).

#### :arrow_forward: Kafka.subscribe(String topic, Class&lt;T> type, BiConsumer&lt;T, Lane> callback) : void

The most important first contact with the *Franz* API is a *subscription*.
The Kafka internals are hidden and the API can be used in a type-safe way.
To subscribe, it is required to provide three parameters:

- The *base topic* defines the topics to subscribe to.
Our library will automatically subscribe to both the `NORMAL` and the `PRIORITY` lane.
- A definition of the *message type*.
Internally, Kafka works with String-based messages, but *Franz* transparently converts data structure from and to JSON using Jackson.
There are multiple overloaded version of the message available, the simplest way is to provide a reference to the *class* of the object that has been serialized, which is illustrated below.
- A *callback* is called for every consumed message.
It is not necessary to distinguish lanes in the subscription, messages on both lanes will trigger the callback.
The deserialized object will be provided as the first argument, the source lane as the second argument.

This basic usage is illustrated in the following snippet.

```java
Kafka k = ...
// Assuming the message is a serialized `Date`
k.subscribe("my-topic", Date.class, (obj, lane) -> {
  // ...
})
```


#### :arrow_forward: Kafka.subscribe(String topic, TRef&lt;T> type, BiConsumer&lt;T, Lane> callback) : void

While providing a *class* for a subscription very often results in a very short notation, some types cannot be expressed in this style, for example, generic types (e.g., `List<String>`).
For broader applicability, the `subscribe` method is overloaded and allows to use an [anonymous class](https://docs.oracle.com/javase/tutorial/java/javaOO/anonymousclasses.html) that extends `TRef` instead.
Apart from this detail, the method is used in the same way as the previous example, as illustrated below.

```java
// Assuming the message is a serialized `List<String>`
k.subscribe("my-topic", new TRef<List<String>>() {}, (obj, lane) -> {
  // ...
})
```

#### :arrow_forward: Kafka.subscribe(String topic, ..., BiFunction&lt;T, Throwable, ?> errors) : void

The execution of any of the registered callbacks might crash.
By *default*, any `Exception` that is thrown in the execution of a subscribed callback is caught and logged to the terminal, but it remains otherwise ignored.
*Franz* assumes that the callbacks handle errors, because they are the information experts.
There are two ways to achieve this.

- Wrapping the callback logic in a `try-catch` block.
Caught exceptions could then, for example, be published to the `ERROR` lane.
- Adding an *error callback* to the subscription to which all caught exceptions will be propagated.
It will also receive the deserialized message as an argument.
An arbitrary data structure can be instantiated by the error callback to represent the exception.
Whatever object is returned will then be serialized with Jackson and published in the `ERROR` lane of the source topic.

The overloaded version of the `subscribe` method that allows to register error handlers is available both for the *class* and for the *TRef* style.
A concrete example of how to register an error callback is provided below.


```java
BiConsumer<Date, Lane> callback = ...
// Assuming the message is a serialized `Date`
k.subscribe("my-topic", Date.class, callback, (obj, err) -> {
  // ...
  return new AnyDataStructureOfChoice(1, "a", new Date());
})
```

#### :arrow_forward: Kafka.publish(T obj, String topic, Lane lane) : void

Three information are required to publish a message in a Kafka topic.

- An instance of an arbitrary data structure.
The object will be serialized to JSON using Jackson before it gets published.
- The *base topic* to which the object should be published.
- The `Lane` on which the object should be published.
To reiterate, internally, *base topic* and `Lane` will be concatenated.

A `publish` can be called in isolation or from within a consuming subscription callback.
The following example illustrates the basic usage in an isolated setting, where a set of strings is published to the `PRIORITY` lane of `my-topic`.

```java
var someObj = Set.of("a", "b", "c");
k.publish(someObj, "my-topic", Lane.PRIORITY);
```

#### :arrow_forward: Kafka.poll() : void

While `publish` calls immediately send messages to the server, `subscribe` calls are not automatically invoked by Kafka.
To start the machinery, it is necessary to `poll` new messages from the Kafka server through repeated calls.
For example, in an endless loop (as illustrated below) or in a loop that has a guarding condition.

Internally, *Franz* will connect two consumers, which are consuming messages on all subscribed topics.
One consumer will listen on all `NORMAL` lanes, the other one on all `PRIORITY` lanes.
Messages on the `PRIORITY` lane always have precedence and will be processed by the consumer as long as more exist.
If no new `PRIORITY` messages can be found, the `NORMAL` lane is polled.
If (and only if) both lanes do not contain any new messages anymore, the polling will wait on the `PRIORITY` lane and only fall back to `NORMAL` every few seconds.
Once either `PRIORITY` or `NORMAL` contain a new message, the wait time is eliminated again.

```java
while(true) {
  k.poll();
}
```

## Advanced Usage

The methods in this category have been introduced to enable advanced Kafka usage.

#### :arrow_forward: Kafka.sendHeartbeat(): void

Kafka's configuration can limit the time that consumers have for processing a message.
If this limit is exceeded, a consumer is considered to be crashed and Kafka will initiate a *rebalancing* of the available *partitions* within the *consumer group*.
This is an expensive operation, so clients should try to avoid it by fine-tuning the *max poll interval* to a suitable number.

*Franz* offers a second solution, i.e., *sending a heartbeat*.
Long-running operations can call the `sendHeartbeat` method to invoke a poll with an empty subscription set.
While this `poll` won't receive new message, it will reset the timer and prevent rebalancing effectively.
It is recommended to combine a conservative, but reasonable *max poll interval* with intermediate *heartbeats* for the most efficient processing.

*Experimental:* Sometimes, operations take too long and it is not possible to added intermediate `sendHeartbeat` calls.
This is addressed in the newest *Franz* releases, which allow to `Kafka.enableBackgroundHeartbeat` and `Kafka.disableBackgroundHeartbeat`, once a `BackgroundHeartbeatHelper` has been registered via `KafkaImpl.setBackgroundHeartbeatHelper`.
This helper will run in a separate thread and will periodically call the `sendHeartbeat` method.
The duration between heartbeats can be specified in the helpers constructor.
Please note though that this feature has not been extensively used in practice so far.


#### :arrow_forward: Kafka.stop() : void

Calling the `stop` method provides a simple means for a graceful shutdown of the Kafka connections.
To be completely safe, a *shutdown hook* should be registered in the runtime, as shown below.

```java
var k = new KafkaImpl(...);
vat t = new KafkaGracefulShutdownThread(k);
Runtime.getRuntime().addShutdownHook(t);
```

#### :arrow_forward: Kafka.commit() : void

This feature is not to be confused with Kafka's built-in auto-commit behavior.
*Franz* will always *disable* the internal auto-commit behavior of Kafka and provide a different notion of *commit*.

The behavior of *Franz* is configured with the `shouldAutoCommit` parameter that is provided in the constructor of `KafkaImpl`.
A sensible default value here is `true`: Once all registered subscriptions have been processed, an *internal Kafka commit* will be triggered, which marks messages as consumed in the server.
This behavior might not be wanted in all use cases though.
For example, as a (Kafka) *commit* is expensive, auto-commits should be avoided for topics with a high throughput.

Auto-commits can be disabled, by setting the `shouldAutoCommit` parameter to `false`, which makes it necessary to manually invoke `commit` to mark messages as consumed.
A connected client will always receive new message in subsequent `poll` calls within the same connection, however, should the client loose connection (e.g., through a crash or restart), the Kafka server will start returning messages from the last committed message again.
A manual invocation of `commit` will move the index pointer on the server to the last consumed message and will persist this information on the server throughout a crash or restart.



## Frequently Asked Questions

#### :question: How do I change or extend the Kafka config?

The configuration that is included in the `KafkaConnector` contains opinionated defaults.
Changing or extending this configuration is easy and only requires the creation of a subclass of `KafkaConnector` that overrides the `getConsumerProperties`/`getProducerProperties` methods.
The return value of said methods is a `Properties` object for which arbitrary properties can be added/removed/changed.
The extended `KafkaConnector` then needs to be provided when instantiating `KafkaImpl` to make use of the new properties.

#### :question: How can I use my own data classes?

*Franz* embraces the use of custom data classes to avoid any manual creation and parsing of JSON from within the business logic of an application.
As such, it is straight forward to introduce custom data structures and (de-)serialize the corresponding JSON:

1. Create the custom data class.
2. Provide a Jackson `Module` for custom (de-)serialization.
3. Register the `Module` with the instance of the `ObjectMapper`.
4. Define message type as `.class` or as `TRef` when using `Kafka.consume`

Steps 2 and 3 are optional.
Depending on the data complexity or (de-)serialization needs, it might not even be necessary to provide an explicit `Module`.
For simple data structures it is often enough to just use a vanilla `ObjectMapper`.

**Please Note:** the [examples](examples) introduce a custom data structure `SomeInputData` and its corresponding (de-)serializers in `SomeInputDataJson`.
The initialization logic in `Main` illustrates how to use all pieces together.


#### :question: How can I change the number of lanes?

The implementation is opinionated and always works with three lanes: `NORMAL`, `PRIORITY`, and `ERROR`.
If you need more lanes, unfortunately, your only option is to fork and extend *Franz*.

In contrast, if you do not need any one of these three, just don't use it.
The only recommendation is that if you only need one lane, use the `PRIORITY` lane (for performance reasons) and ignore the `NORMAL` lane.
Please note that messages with caught exceptions (see `subscribe` with error handler) will be published in the `ERROR` lane of the originating topic.



#### :question: How can access messgaes that have been posted in an `ERROR` lane?

The `Kafka` interface was designed for regular workloads and it does not provide the means to access messages in `ERROR` lanes.
Instead, of using the regular `subscribe` and `poll` methods of the `Kafka` interface, clients need to work with the `KafkaErrors` interface and use the `subscribeErrors` and `pollAllErrors` methods.
While the former is used similarly to the other subscribe methods, `pollAllErrors` does not require a loop and will *always* process *all* error messages on the subscribed topics.


#### :question: How can I change the suffixes of a combined topic or use no suffix at all?

The different lanes are distinguished by appending a lane name to a *base topic*.
This logic is implemented in `KafkaImpl.getExtension(Lane)`.
To change the mapping, you can extend the class and simply use altered (or empty) suffixes for the three lanes.
The only requirement is that all three lanes remain distinguishable.

