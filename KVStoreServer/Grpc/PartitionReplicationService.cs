using Common.Protos.Replication;
using Grpc.Core;
using System.Threading.Tasks;

using KVStoreServer.Communications;

using static Common.Protos.Replication.ReplicationService;
using System;

namespace KVStoreServer.Grpc {
    class PartitionReplicationService : ReplicationServiceBase {

        private readonly RequestsDispatcher dispatcher;
        public PartitionReplicationService(RequestsDispatcher dispatcher) {
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

        private LockArguments ParseLockRequest(LockRequest request) {
            return new LockArguments {
                PartitionName = request.PartitionName,
                ObjectId = request.ObjectId
            };
        }

        private WriteObjectArguments ParseWriteRequest(WriteRequest request) {
            return new WriteObjectArguments {
                PartitionName = request.PartitionName,
                ObjectId = request.ObjectId,
                ObjectValue = request.ObjectValue
            };
        }
    }
}
