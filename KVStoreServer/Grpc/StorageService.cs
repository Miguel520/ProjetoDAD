using System.Threading.Tasks;
using Common.Protos.KeyValueStore;
using Grpc.Core;

namespace KVStoreServer.Grpc {
    public class StorageService : KeyValueStoreService.KeyValueStoreServiceBase {
        public StorageService() {
        }

        public override Task<ReadResponse> Read(ReadRequest request, ServerCallContext context) {
            return base.Read(request, context);
        }

        public override Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            return base.Write(request, context);
        }

        public override Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            return base.List(request, context);
        }
    }
}
