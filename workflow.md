# Questions

# FsKafka

## FsKafka.Connection

 Config, type and helper functions for connecting to kafka servers, keeping
 connections alive and requesting data from servers

## FsKafka.Producer

 Config, type and helper functions for producing messages to kafka

## FsKafka.Consumer

 Config, type and helper functions for consuming messages from kafka

## FsKafka.Codec.Pickle

 Primitive types serializers with combinators for building more complex type
 serializers

## FsKafka.Codec.Unpickle

 Primitive types deserializers and combinators with computation expression
 builder for building more complex type deserializers

## FsKafka.Codec.Crc32

 Crc32 hash calculator for Message validation

## FsKafka.Codec.Compression

 Wrappers for GZip and Snappy compressions.

## FsKafka.Protocol

 Implementation of apache kafka protocol. Just simple translation from BNF used
 in official documentation to F# types.

## FsKafka.Protocol.Request

 Creating requests from required data without boilerplate of creating each inner
 types that represent full request and function to encode request to kafka
 network binary format

## FsKafka.Protocol.Response

 Decoding responses from kafka network binary format to F# representation using
 types defined in Protocol namespace
