using Common.Protos.NamingService;
using Grpc.Core;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using KVStoreServer.Replication;

using static Common.Protos.NamingService.NamingService;

namespace KVStoreServer.Grpc.Base {
    class NamingService : NamingServiceBase {

        private readonly BaseIncomingDispatcher dispatcher;

        public NamingService(BaseIncomingDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public override async Task<LookupResponse> Lookup(LookupRequest request, ServerCallContext context) {
            string serverId = request.ServerId;
            string serverUrl = await dispatcher.OnLookup(serverId);
            if (serverUrl != null) {
                return new LookupResponse { ServerUrl = serverUrl };
            }
            throw new RpcException(new Status(StatusCode.NotFound, "No Such Id"));
        }

        public override async Task<LookupMasterResponse> LookupMaster
            (LookupMasterRequest request, 
            ServerCallContext context) {
            
            string partitionName = request.PartitionId;
            string masterUrl = await dispatcher.OnLookupMaster(partitionName);
            if (masterUrl != null) {
                return new LookupMasterResponse { MasterUrl = masterUrl };
            }
            throw new RpcException(new Status(StatusCode.NotFound, "No Such Partition"));
        }

        public override async Task<ListPartitionsResponse> ListPartitions(ListPartitionsRequest request, ServerCallContext context) {
            IEnumerable<PartitionServersDto> listPartitions = await dispatcher.OnListPartitions();
            return new ListPartitionsResponse {
                Partitions = { BuildPartition(listPartitions) }
            };
        }

        private static IEnumerable<Partition> BuildPartition(IEnumerable<PartitionServersDto> listPartitions) {
            return listPartitions.Select(obj => {
                Partition p = new Partition {
                    PartitionId = obj.PartitionId,
                };
                p.ServerIds.AddRange(obj.ServerIds);
                return p;
            });
        }
    }
}
