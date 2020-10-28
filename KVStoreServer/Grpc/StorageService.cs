using System;
using System.Threading.Tasks;
using Common.Protos.KeyValueStore;
using Grpc.Core;

using KVStoreServer.Communications;

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

        public override Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            return base.List(request, context);
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
    }
}
