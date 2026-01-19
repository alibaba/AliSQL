<xsl:stylesheet version="3.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:xs="http://www.w3.org/2001/XMLSchema"
  exclude-result-prefixes="xs">

  <xsl:variable name="emphasized-template-parameter-types" select="
    'Allocator',
    'AsyncStream',
    'AsyncReadStream',
    'AsyncWriteStream',
    'Body',
    'BufferSequence',
    'BufferSequence',  (: TODO: Was this intended to be 'BufferSequence_' ?? :)
    'CompletionCondition',
    'CompletionHandler',
    'CompletionToken',
    'ConnectCondition',
    'ConnectHandler',
    'ConstBufferSequence',
    'DynamicBuffer',
    'EndpointSequence',
    'ExecutionContext',
    'Executor',
    'Executor_',
    'Executor1',
    'Executor2',
    'Fields',
    'Handler',
    'Handler_',
    'IteratorConnectHandler',
    'MutableBufferSequence',
    'Protocol',
    'RangeConnectHandler',
    'RatePolicy',
    'ReadHandler',
    'Stream',
    'SyncStream',
    'SyncReadStream',
    'SyncWriteStream',
    'WriteHandler'
  "/>

</xsl:stylesheet>
