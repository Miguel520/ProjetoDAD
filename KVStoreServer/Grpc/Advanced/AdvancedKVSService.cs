
using Common.Protos.AdvancedKeyValueStore;
using Grpc.Core;
using System.Collections.Generic;
using System.Threading.Tasks;

using static Common.Protos.AdvancedKeyValueStore.AdvancedKeyValueStoreService;

namespace KVStoreServer.Grpc.Advanced {
    class AdvancedKVSService : AdvancedKeyValueStoreServiceBase {

        private readonly AdvancedIncomingDispatcher dispatcher;

        public AdvancedKVSService(AdvancedIncomingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override Task<ReadResponse> Read(ReadRequest request, ServerCallContext context) {
            return base.Read(request, context);
        }

        public override async Task<WriteResponse> Write(WriteRequest request, ServerCallContext context) {
            CausalConsistency.ImmutableVectorClock timestamp =
                await dispatcher.OnWrite(ParseWriteRequest(request));
            return new WriteResponse {
                Timestamp = BuildClock(timestamp)
            };
        }

        public override Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            return base.List(request, context);
        }

        private WriteArguments ParseWriteRequest(WriteRequest request) {
            return new WriteArguments {
                PartitionId = request.PartitionId,
                ObjectId = request.ObjectId,
                ObjectValue = request.ObjectValue,
                Timestamp = BuildVectorClock(request.Timestamp)
            };
        }

        private CausalConsistency.ImmutableVectorClock BuildVectorClock(VectorClock vectorClock) {
            List<KeyValuePair<string, int>> clocks = new List<KeyValuePair<string, int>>();
            for (int i = 0; i < vectorClock.ServerIds.Count; i++) {
                clocks.Add(new KeyValuePair<string, int>(
                    vectorClock.ServerIds[i],
                    vectorClock.ServerClocks[i]
                ));
            }
            return CausalConsistency.ImmutableVectorClock.FromClocks(clocks);
        }

        private VectorClock BuildClock(CausalConsistency.ImmutableVectorClock vectorClock) {
            List<string> serverIds = new List<string>();
            List<int> clocks = new List<int>();
            foreach ((string serverId, int serverClock) in vectorClock.Clocks) {
                serverIds.Add(serverId);
                clocks.Add(serverClock);
            }
            return new VectorClock {
                ServerIds = { serverIds },
                ServerClocks = { clocks }
            };
        }
    }
}
