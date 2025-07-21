from mister_webhooks import ConnectionProfile, MisterWebhooksConsumer
import sys, pprint

if __name__ == "__main__":
    topic, profile_path = sys.argv[1:]

    profile = ConnectionProfile.from_file(profile_path)
    consumer = MisterWebhooksConsumer(topic, profile)

    consumer.run(pprint.pprint)
