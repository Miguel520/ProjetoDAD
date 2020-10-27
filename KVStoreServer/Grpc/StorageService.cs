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

        public override Task<ReadResponse> Read(ReadRequest request, ServerCallContext context) {
            return base.Read(request, context);
        }

        public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            await dispatcher.Write(ParseWriteRequest(request));
            return new WriteResponse();
        }

        public override Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            return base.List(request, context);
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
