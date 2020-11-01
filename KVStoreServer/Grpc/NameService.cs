using Common.Protos.NamingService;
using Grpc.Core;
using System.Threading.Tasks;

using KVStoreServer.Replication;

using static Common.Protos.NamingService.NamingService;
using System.Collections.Generic;
using System.Linq;
using KVStoreServer.Communications;
using System;

namespace KVStoreServer.Grpc {
    class NamingService : NamingServiceBase {

        private readonly PartitionsDB dB;

        public NamingService(PartitionsDB dB) {
            this.dB = dB;
        }

        public override Task<LookupResponse> Lookup(LookupRequest request, ServerCallContext context) {
            int serverId = request.ServerId;
            if (dB.TryGetUrl(serverId, out string serverUrl)) {
                return Task.FromResult(new LookupResponse { ServerUrl = serverUrl });
            }
            throw new RpcException(new Status(StatusCode.NotFound, "No Such Id"));
        }

        public override Task<LookupMasterResponse> LookupMaster
            (LookupMasterRequest request, 
            ServerCallContext context) {
            
            string partitionName = request.PartitionName;
            if (dB.TryGetMaster(partitionName, out int masterId)
                && dB.TryGetUrl(masterId, out string masterUrl)) {

                return Task.FromResult(new LookupMasterResponse { MasterUrl = masterUrl });
            }
            throw new RpcException(new Status(StatusCode.NotFound, "No Such Partition"));
        }

        public override Task<ListPartitionsResponse> ListPartitions(ListPartitionsRequest request, ServerCallContext context) {
            IEnumerable<PartitionServersDto> listPartitions = dB.ListPartitionsWithServerIds();
            return Task.FromResult(new ListPartitionsResponse
            {
                Partitions = { BuildPartition(listPartitions) }
            });
        }

        private static IEnumerable<Partition> BuildPartition(IEnumerable<PartitionServersDto> listPartitions) {
            return listPartitions.Select(obj => {
                Partition p = new Partition
                {
                    PartitionName = obj.PartitionName,
                };
                p.ServerIds.AddRange(obj.ServerIds);
                return p;
            });
        }
    }
}
