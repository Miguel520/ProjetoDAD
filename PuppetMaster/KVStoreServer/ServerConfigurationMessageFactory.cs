using Common.Protos.ServerConfiguration;
using System;
using System.Linq;
using System.Collections.Generic;

namespace PuppetMaster.KVStoreServer {

    public class ServerConfigurationMessageFactory {
        private ServerConfigurationMessageFactory() {
        }

        public static JoinPartitionRequest BuildJoinPartitionRequest(
            string partitionName,
            IEnumerable<Tuple<int,string>> servers,
            int masterId) {

            return new JoinPartitionRequest {
                PartitionName = partitionName,
                Servers = { BuildServers(servers) },
                MasterId = masterId
            };
        }

        private static IEnumerable<Server> BuildServers(
            IEnumerable<Tuple<int, string>> servers) {

            return servers.Select(tuple => {
                return new Server {
                    Id = tuple.Item1,
                    Url = tuple.Item2
                };
            });
        }
    }
}
