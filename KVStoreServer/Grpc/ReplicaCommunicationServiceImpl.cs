using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System.Threading.Tasks;

using KVStoreServer.Communications;

using static Common.Protos.ReplicaCommunication.ReplicaCommunicationService;

namespace KVStoreServer.Grpc {
    class ReplicaCommunicationServiceImpl : ReplicaCommunicationServiceBase {

        private readonly RequestsDispatcher dispatcher;
        public ReplicaCommunicationServiceImpl(RequestsDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<LockResponse> Lock(LockRequest request, ServerCallContext context) {
            await dispatcher.Lock(ParseLockRequest(request));
            return new LockResponse { };
        }

        public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            await dispatcher.WriteObject(ParseWriteRequest(request));
            return new WriteResponse { };
        }

        public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context) {
            await dispatcher.Ping();
            return new PingResponse { };
        }

        private LockArguments ParseLockRequest(LockRequest request) {
            return new LockArguments {
                PartitionId = request.PartitionId,
                ObjectId = request.ObjectId
            };
        }

        private WriteObjectArguments ParseWriteRequest(WriteRequest request) {
            return new WriteObjectArguments {
                PartitionId = request.PartitionId,
                ObjectId = request.ObjectId,
                ObjectValue = request.ObjectValue
            };
        }
    }
}
