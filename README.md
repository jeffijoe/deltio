# Deltio

A Google Cloud Pub/Sub emulator alternative for local development.

> ℹ️ **DISCLAIMER**: This project is not endorsed, sponsored, or affiliated with Google Cloud and/or the Rust Foundation.

### Why?

Performance.

The official Google Cloud Pub/Sub emulator would make our machines come to a crawl under moderate load (for example, 
integration testing with >50 topics + subscriptions). Even after the tests were done, the emulator would still be 
spinning the CPU. Frequent restarts were needed, as performance degraded over time.   

Deltio is a minimal implementation of a Google Cloud Pub/Sub emulator that supports the core features needed
to use Pub/Sub.

# Installation

You can either:

* [Download the latest release](https://github.com/jeffijoe/deltio/releases/latest) for your platform.
* Use Docker:

  ```bash
  docker run -p 8085:8085 ghcr.io/jeffijoe/deltio:latest
  ```

# Running

Assuming you have placed `deltio` somewhere in your `$PATH`, run Deltio with the default options (port: `8085`):

```bash
$ deltio
```

To use a different port:

```bash
$ deltio --bind 0.0.0.0:1337
```

To see a list of options:

```bash
$ deltio --help
```

# Limitations

As of this time, Deltio has **very limited functionality**, much less than the official Google Cloud Pub/Sub emulator.

**Currently supported features:**

* Create topic
  * Only the `name` property is respected
* Get topic
* List topics in a project
* Publish messages
  * Only the `data` property is respected
* Delete topics
* Create subscription
  * Supports `name` and `ack_deadline_seconds` properties
* Get subscription
* List subscriptions in a project
* List subscription names in a topic
* Modify ACK deadlines
* ACK messages
* Pull messages
* Streaming-pull messages
  * Including handling stream requests for acks and deadline modifications
  * Does **NOT** support flow control properties
* Message expiration
* Deleting subscriptions

If something is not listed above, it's probably not supported yet. For example: message ordering, exactly-once delivery, deadletter and expiration policies, are not supported.

# Compiling from source

Deltio is written in Rust, and requires a Protocol Buffers compiler. This is because the [official Google Cloud Pub/Sub protos](https://github.com/googleapis/googleapis/blob/master/google/pubsub/v1/pubsub.proto) are used to generate the server code.

With both of those configured, you can simply run:

```bash
cargo build --release
```

# What's in a name?

> In Greek, the term "deltio" (δελτίο) translates to "bulletin" or "announcement." It is commonly used to refer to a document or publication that provides information, updates, or news about a particular topic. For example, a "deltio" can be a newsletter, a news bulletin, or an official communication issued by an organization or government entity.
>
>~ ChatGPT

# Author

Jeff Hansen - [@Jeffijoe](https://twitter.com/Jeffijoe)