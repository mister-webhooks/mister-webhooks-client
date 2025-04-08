import { Kafka, Consumer, logLevel } from 'kafkajs'
import CBOR, { comment } from 'cbor2';
import { program } from 'commander';
import fs from 'node:fs';

export type WebhookCallback<T> = (parameters: { topic: string, partition: number, offset: bigint, key: string, headers: Map<string, string[]>, message: T }) => void

class MisterWebhooksConnection {
  readonly consumerName: string;
  readonly topic: string;
  readonly kafka: Kafka;

  constructor(filename: string, logLevel: logLevel) {
    const data = JSON.parse(fs.readFileSync(filename).toString())

    const consumerName: string = data['consumer_name']
    const topic: string = data['kafka']['topic']
    const authSecret: string = data['auth']['secret']
    const authMechanism: "plain" = data['auth']['mechanism']
    const bootstrapServers: Array<string> = data['kafka']['servers']

    this.consumerName = consumerName
    this.topic = topic

    this.kafka = new Kafka({
      clientId: consumerName,
      brokers: bootstrapServers,
      sasl: {
        mechanism: authMechanism,
        username: consumerName,
        password: authSecret
      },
      logLevel: logLevel,
    });
  }
}

var activeConsumers: MisterWebhooksConsumer<any>[] = []
const shutdownSignals: NodeJS.Signals[] = ['SIGINT', 'SIGTERM']

shutdownSignals.forEach(signal =>
  process.on(signal, async () => {
    console.info("received stop signal, shutting down")

    for (let activeConsumer of activeConsumers.slice()) {
      await activeConsumer.shutdown()
    }
    process.exit(0);
  })
)

export class MisterWebhooksConsumer<T> {
  private c: Consumer | null;
  private conn: MisterWebhooksConnection;

  constructor(conn: MisterWebhooksConnection) {
    this.c = null;
    this.conn = conn;
  }

  async consume(callback: WebhookCallback<T>): Promise<void> {
    type WebhookPayload<T> = {
      body: T,
      headers: Map<string, string[]>,
    }

    const kafka = this.conn.kafka;

    this.c = kafka.consumer({ groupId: this.conn.consumerName })

    await this.c.connect()
    await this.c.subscribe({ topic: this.conn.topic, fromBeginning: true })

    activeConsumers.push(this);

    console.info("starting consume loop")

    await this.c.run({
      eachMessage: async ({ topic, partition, message, heartbeat }) => {

        const decoded = <WebhookPayload<T>>CBOR.decode(message.value!);

        if (decoded == undefined) {
          throw new Error(`Kafka message ${topic}.${partition}[${message.offset}] could not be decoded`)
        }

        callback({
          topic: topic,
          partition: partition,
          offset: BigInt(message.offset),
          key: message.key!.toString(),
          headers: decoded.headers,
          message: decoded.body
        })
      }
    })
  }

  async shutdown() {
    if (this.c) {
      await this.c.disconnect();
      activeConsumers = activeConsumers.filter((c) => c != this)
    }
  }
}

const parseLogLevel = (levelName: string): logLevel => {
  switch (levelName.toLowerCase()) {
    case "nothing": return logLevel.NOTHING;
    case "error": return logLevel.ERROR;
    case "warn": return logLevel.WARN;
    case "info": return logLevel.INFO;
    case "debug": return logLevel.DEBUG;
    default:
      throw new Error(`Configuration error: ${levelName} is not recognized`)
  }
}

program
  .command("reset_to_beginning <connfile>")
  .description("reset the consumer to start from the beginning")
  .option('--log-level', 'Logging level', parseLogLevel, logLevel.NOTHING)
  .action(async (connfile, cmd) => {
    const conn = new MisterWebhooksConnection(connfile, cmd.logLevel)

    let a = conn.kafka.admin()

    await a.connect()

    await a.resetOffsets({ groupId: conn.consumerName, topic: conn.topic, earliest: true })
    await a.disconnect()
    console.info(`reset offets on ${conn.topic} for ${conn.consumerName}`)
  })

program
  .command("log <connfile>")
  .description("run the consumer against <topic>")
  .option('--log-level', 'Logging level', parseLogLevel, logLevel.INFO)
  .action(async (connfile, cmd) => {
    await new MisterWebhooksConsumer<any>(new MisterWebhooksConnection(connfile, cmd.logLevel))
      .consume(console.info)
  })


program.parseAsync()