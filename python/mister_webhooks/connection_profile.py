from dataclasses import dataclass
import io
import json
from typing import Literal


@dataclass
class ConnectionProfile:
    """ConnectionProfile represents the data within a Mister Webhooks connection profile. A ConnectionProfile is usually
    created using one of the static methods, and then used to instantiate a MisterWebhooksConsumer.

    Attributes:
      consumer_name (str): Name of this consumer.
      auth_mechanism (Literal: 'plain'): SASL authentication mechanism this consumer uses.
      auth_secret: (str): The secret to be used in SASL authentication.
      kafka_bootstrap: (str): Kafka DNS domain & port to use to find brokers.
    """

    consumer_name: str
    auth_mechanism: Literal["plain"]
    auth_secret: str
    kafka_bootstrap: str

    @staticmethod
    def from_file(path) -> "ConnectionProfile":
        """from_file reads a ConnectionProfile from a Mister Webhooks connection profile file."""
        with io.open(path, "r") as f:
            data = json.load(f)

            return ConnectionProfile(
                data["consumer_name"],
                data["auth"]["mechanism"],
                data["auth"]["secret"],
                data["kafka"]["bootstrap"],
            )
