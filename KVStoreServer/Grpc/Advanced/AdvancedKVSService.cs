
using Common.Protos.AdvancedKeyValueStore;
using Grpc.Core;
using KVStoreServer.Storage.Advanced;
using System.Collections.Generic;
using System.Linq;
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

        public override async Task<ListResponse> List(ListRequest request, ServerCallContext context) {
            IEnumerable<StoredObjectDto> objects = await dispatcher.OnListServer();
            return new ListResponse {
                Objects = { BuildObject(objects) }
            };
        }

        private WriteArguments ParseWriteRequest(WriteRequest request) {
            return new WriteArguments {
                PartitionId = request.PartitionId,
                ObjectId = request.ObjectId,
                ObjectValue = request.ObjectValue,
                Timestamp = BuildVectorClock(request.Timestamp)
            };
        }

        private IEnumerable<StoredObject> BuildObject(IEnumerable<StoredObjectDto> objectDtos) {
            return objectDtos.Select(objectDto => {
                return new StoredObject {
                    PartitionId = objectDto.PartitionId,
                    ObjectId = objectDto.ObjectId,
                    ObjectValue = objectDto.TimestampedValue.Value,
                    ObjectTimestamp = BuildClock(objectDto.TimestampedValue.Timestamp),
                    ObjectLastWriteServerId = objectDto.TimestampedValue.LastWriteServerId
                };
            });
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
