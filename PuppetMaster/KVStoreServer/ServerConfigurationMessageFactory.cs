using Common.Protos.ServerConfiguration;
using System;
using System.Linq;
using System.Collections.Generic;

namespace PuppetMaster.KVStoreServer {

    public class ServerConfigurationMessageFactory {
        private ServerConfigurationMessageFactory() {
        }

        public static JoinPartitionRequest BuildJoinPartitionRequest(
            string partitionId,
            IEnumerable<Tuple<string,string>> servers,
            string masterId) {

            return new JoinPartitionRequest {
                PartitionId = partitionId,
                Servers = { BuildServers(servers) },
                MasterId = masterId
            };
        }

        private static IEnumerable<Server> BuildServers(
            IEnumerable<Tuple<string, string>> servers) {

            return servers.Select(tuple => {
                return new Server {
                    Id = tuple.Item1,
                    Url = tuple.Item2
                };
            });
        }
    }
}
