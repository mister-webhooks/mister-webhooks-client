import avro from 'avsc'
import * as CBOR from 'cbor2'
import { KafkaMessage } from 'kafkajs'

type EnvelopeBase = {
  method: 'GET' | 'HEAD' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  headers: Record<string, string[]>
}

type CBOREnvelope = EnvelopeBase & {
  encoding: 'CBOR'
  payload: Uint8Array
}

type JSONEnvelope = EnvelopeBase & {
  encoding: 'JSON'
  payload: string
}

const KNOWN_ENVELOPE_TYPE = 0x80

type EnvelopeV1Message = CBOREnvelope | JSONEnvelope
export const KafkaMessageEnvelopeV1Schema = avro.Type.forSchema({
  type: 'record',
  name: 'KafkaMessageEnvelopeV1',
  namespace: 'com.mister_webhooks.data',
  fields: [
    {
      name: 'method',
      type: {
        type: 'enum',
        name: 'Methods',
        symbols: ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'PATCH'],
      },
    },
    {
      name: 'headers',
      type: {
        type: 'map',
        values: {
          type: 'array',
          items: 'string',
        },
      },
    },
    {
      name: 'payload',
      type: 'bytes',
    },
    {
      name: 'encoding',
      type: {
        type: 'enum',
        name: 'Encodings',
        symbols: ['JSON', 'CBOR'],
      },
    },
  ],
})

type DecodeMessageResult<T> = {
  decoded: T
  method: EnvelopeBase['method']
  headers: EnvelopeBase['headers']
} | null

const decodeCbor = (value: Uint8Array) => {
  return CBOR.decode(value)
}

const decodeJson = (value: string): unknown => {
  return JSON.parse(value)
}

class MalformedMessageError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'MalformedMessageError'
  }
}

export const decodeMessage = <T>(kafkaMessage: KafkaMessage): DecodeMessageResult<T> => {
  if (!kafkaMessage.headers?.envelope) {
    throw new MalformedMessageError("malformed Kafka message, 'envelope' header is missing")
  }

  const envelopeType = kafkaMessage.headers.envelope[0]
  if (typeof envelopeType !== 'number') {
    const typeName = typeof envelopeType === 'string' ? 'string' : 'Buffer'
    throw new MalformedMessageError(
      `expected 'envelope' header to be single byte, got ${typeName} instead`
    )
  }

  if (envelopeType > KNOWN_ENVELOPE_TYPE) {
    throw new MalformedMessageError(
      `envelope type ${envelopeType.toString()} is unsupported, please upgrade mister-webhooks-client`
    )
  }

  if (envelopeType < KNOWN_ENVELOPE_TYPE) {
    return null
  }

  if (!kafkaMessage.value) {
    throw new MalformedMessageError('Invalid message value')
  }

  const envelope: EnvelopeV1Message = KafkaMessageEnvelopeV1Schema.fromBuffer(
    kafkaMessage.value
  ) as EnvelopeV1Message

  const payload =
    envelope.encoding === 'CBOR' ? decodeCbor(envelope.payload) : decodeJson(envelope.payload)

  return {
    decoded: payload as T,
    headers: envelope.headers,
    method: envelope.method,
  }
}
