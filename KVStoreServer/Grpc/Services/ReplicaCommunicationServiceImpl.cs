using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.ReplicaCommunicationService;

namespace KVStoreServer.Grpc {
    class ReplicaCommunicationServiceImpl : ReplicaCommunicationServiceBase {

        private readonly SimpleIncomingDispatcher dispatcher;
        public ReplicaCommunicationServiceImpl(SimpleIncomingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<LockResponse> Lock(LockRequest request, ServerCallContext context) {
            await dispatcher.OnLock(ParseLockRequest(request));
            return new LockResponse { };
        }

        public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            await dispatcher.OnWriteObject(ParseWriteRequest(request));
            return new WriteResponse { };
        }

        public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context) {
            await dispatcher.OnPing();
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
