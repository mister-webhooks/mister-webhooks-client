The Mister Webhooks API is organized around the `MisterWebhooksConsumer` class. Its sole purpose is
to support writing event consumers easily. The result is a minimal API.

Here's an example of the smallest possible Mister Webhooks client:

```python
from mister_webhooks import ConnectionProfile, MisterWebhooksConsumer
import sys, pprint

if __name__ == "__main__":
    topic, profile_path = sys.argv[1:]

    profile = ConnectionProfile.from_file(profile_path)
    consumer = MisterWebhooksConsumer(topic, profile)

    consumer.run(pprint.pprint)
```

This program takes the topic to consume from as its first command-line argument and the path to the connection profile file as its second. It loads the connection profile from disk, uses it to create a consumer, and then
runs the consumer loop, pretty-printing every webhook event as it comes in.

::: mister_webhooks
