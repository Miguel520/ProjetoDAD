using Common.Exceptions;
using Common.Protos.SimpleKeyValueStore;
using Grpc.Core;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using KVStoreServer.Storage;

namespace KVStoreServer.Grpc.Simple {
    public class SimpleStorageService : SimpleKeyValueStoreService.SimpleKeyValueStoreServiceBase {

        private readonly SimpleIncomingDispatcher dispatcher;
        public SimpleStorageService(SimpleIncomingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<ReadResponse> Read(ReadRequest request, ServerCallContext context) {
            string value = await dispatcher.OnRead(ParseReadRequest(request));
            if (value == null) throw new RpcException(new Status(StatusCode.NotFound, "No such object"));
            return new ReadResponse { 
                ObjectValue = value
            };
        }

        public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            try {
                await dispatcher.OnWrite(ParseWriteRequest(request));
            }
            catch (ReplicaFailureException) {
                throw new RpcException(new Status(StatusCode.Unavailable, "Failed to connect to replica"));
            }
            return new WriteResponse();
        }

        public override async Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            IEnumerable<StoredValueDto> list = await dispatcher.OnListServer();
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
