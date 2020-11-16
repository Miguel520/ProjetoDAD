﻿using Common.Exceptions;
using Common.Protos.KeyValueStore;
using Grpc.Core;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using KVStoreServer.Communications;
using KVStoreServer.Storage;

namespace KVStoreServer.Grpc {
    public class StorageService : KeyValueStoreService.KeyValueStoreServiceBase {

        private readonly RequestsDispatcher dispatcher;
        public StorageService(RequestsDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<ReadResponse> Read(ReadRequest request, ServerCallContext context) {
            string value = await dispatcher.Read(ParseReadRequest(request));
            if (value == null) throw new RpcException(new Status(StatusCode.NotFound, "No such object"));
            return new ReadResponse { 
                ObjectValue = value
            };
        }

        public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            try {
                await dispatcher.Write(ParseWriteRequest(request));
            }
            catch (ReplicaFailureException) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Failed to connect to replica"));
            }
            return new WriteResponse();
        }

        public override async Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            IEnumerable<StoredValueDto> list = await dispatcher.List();
            return new ListResponse {
                Objects = { BuildStoredObjects(list) }
                };
        }

        private static IEnumerable<StoredObject> BuildStoredObjects(
           IEnumerable<StoredValueDto> storedObjects) {

            return storedObjects.Select(obj => {
                return new StoredObject {
                    PartitionId = obj.PartitionId,
                    ObjectId = obj.ObjectId,
                    Value =  obj.Value,
                    IsMaster = obj.IsMaster,
                    IsLocked = obj.IsLocked
                };
            });
        }

        private ReadArguments ParseReadRequest(ReadRequest request) {
            return new ReadArguments {
                PartitionId = request.PartitionId,
                ObjectId = request.ObjectId
            };
        }

        private WriteArguments ParseWriteRequest(WriteRequest request) {
            return new WriteArguments {
                PartitionId = request.PartitionId,
                ObjectId = request.ObjectId,
                ObjectValue = request.ObjectValue
            };
        }
    }
}
