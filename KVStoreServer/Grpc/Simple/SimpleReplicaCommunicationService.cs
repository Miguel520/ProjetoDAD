using Common.Protos.ReplicaCommunication;
using Grpc.Core;
using System.Threading.Tasks;

using static Common.Protos.ReplicaCommunication.SimpleReplicaCommunicationService;

namespace KVStoreServer.Grpc.Simple {
    class SimpleReplicaCommunicationService : SimpleReplicaCommunicationServiceBase {

        private readonly SimpleIncomingDispatcher dispatcher;
        public SimpleReplicaCommunicationService(SimpleIncomingDispatcher dispatcher) {
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
