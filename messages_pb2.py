# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0emessages.proto\"7\n\x11storedata_request\x12\x10\n\x08\x66ilename\x18\x01 \x01(\t\x12\x10\n\x08\x66iledata\x18\x02 \x01(\x0c\"m\n\x0fgetdata_request\x12\x15\n\rfragmentName1\x18\x01 \x01(\t\x12\x15\n\rfragmentName2\x18\x02 \x01(\t\x12\x15\n\rfragmentName3\x18\x03 \x01(\t\x12\x15\n\rfragmentName4\x18\x04 \x01(\t\"\xca\x01\n\x10getdata_response\x12\x15\n\rfragmentName1\x18\x01 \x01(\t\x12\x15\n\rfragmentData1\x18\x02 \x01(\x0c\x12\x15\n\rfragmentName2\x18\x03 \x01(\t\x12\x15\n\rfragmentData2\x18\x04 \x01(\x0c\x12\x15\n\rfragmentName3\x18\x05 \x01(\t\x12\x15\n\rfragmentData3\x18\x06 \x01(\x0c\x12\x15\n\rfragmentName4\x18\x07 \x01(\t\x12\x15\n\rfragmentData4\x18\x08 \x01(\x0c\"/\n\theartbeat\x12\x0f\n\x07node_id\x18\x01 \x01(\t\x12\x11\n\ttimestamp\x18\x02 \x01(\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'messages_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_STOREDATA_REQUEST']._serialized_start=18
  _globals['_STOREDATA_REQUEST']._serialized_end=73
  _globals['_GETDATA_REQUEST']._serialized_start=75
  _globals['_GETDATA_REQUEST']._serialized_end=184
  _globals['_GETDATA_RESPONSE']._serialized_start=187
  _globals['_GETDATA_RESPONSE']._serialized_end=389
  _globals['_HEARTBEAT']._serialized_start=391
  _globals['_HEARTBEAT']._serialized_end=438
# @@protoc_insertion_point(module_scope)
