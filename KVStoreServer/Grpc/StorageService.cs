using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Common.Protos.KeyValueStore;
using Grpc.Core;

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
            await dispatcher.Write(ParseWriteRequest(request));
            return new WriteResponse();
        }

        public override async Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            IEnumerable<StoredValueDto> list = await dispatcher.List();
            return new ListResponse {
                Objects = { BuildStoredObjects(list) }
                };
        }

        public override async Task<ListIdsResponse> ListIds(ListIdsRequest request, ServerCallContext context) {
            IEnumerable<StoredValueDto> list = await dispatcher.ListGlobal(ParseListIdsrequest(request));
            return new ListIdsResponse {
                Ids = { BuildIdentifierObjects(list) }
            };
        }

        private static IEnumerable<StoredObject> BuildStoredObjects(
           IEnumerable<StoredValueDto> storedObjects) {

            return storedObjects.Select(obj => {
                return new StoredObject {
                    PartitionName = obj.PartitionName,
                    ObjectId = obj.ObjectId,
                    Value =  obj.Value,
                    IsMaster = obj.IsMaster,
                    IsLocked = obj.IsLocked
                };
            });
        }

        private static IEnumerable<Identifier> BuildIdentifierObjects(IEnumerable<StoredValueDto> storedObjects) {
            return storedObjects.Select(obj => {
                return new Identifier {
                    PartitionName = obj.PartitionName,
                    ObjectId = obj.ObjectId
                };
            });
        }

        private ReadArguments ParseReadRequest(ReadRequest request) {
            return new ReadArguments {
                PartitionName = request.PartitionName,
                ObjectId = request.ObjectId
            };
        }

        private WriteArguments ParseWriteRequest(WriteRequest request) {
            return new WriteArguments {
                PartitionName = request.PartitionName,
                ObjectId = request.ObjectId,
                ObjectValue = request.ObjectValue
            };
        }

        private ListIdsArguments ParseListIdsrequest(ListIdsRequest request) {
            return new ListIdsArguments {
                PartitionName = request.PartitionName
            };
        }
    }
}
