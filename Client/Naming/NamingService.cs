using Common.Exceptions;
using System;
using System.Collections.Immutable;
using System.Collections.Generic;
using System.Linq;
using Client.Grpc;

namespace Client.Naming {
    public class NamingService {

        private readonly HashSet<string> nameServersUrls;
        private readonly Dictionary<string, string> knownServers;
        private readonly Dictionary<string, string> knownMasters;
        private readonly ImmutableDictionary<string, ImmutableHashSet<string>> partitions;
        private readonly HashSet<string> crashedUrls;

        public NamingService(ImmutableList<string> receivedNameServersUrls) {
            GrpcMessageLayer.ReplicaFailureEvent += AddCrashed;
            
            nameServersUrls = new HashSet<string>(receivedNameServersUrls);
            knownServers = new Dictionary<string, string>();
            knownMasters = new Dictionary<string, string>();
            crashedUrls = new HashSet<string>();
            partitions = ImmutableDictionary.Create<string, ImmutableHashSet<string>>();
            
            foreach (string nameServerUrl in receivedNameServersUrls) {
                if (GrpcMessageLayer.Instance.ListPartitions(nameServerUrl, out partitions)) {
                    break;
                }
            }
        }

        public ImmutableList<string> ServersIds {
            get {
                return partitions.SelectMany(partition => partition.Value).Distinct().ToImmutableList();
            }
        }

        /*
         * Lookup for the url of the server with the given id
         */
        public bool Lookup(string serverId, out string serverUrl) {
            if (knownServers.TryGetValue(serverId, out serverUrl)) {
                // Check if crashed
                if (crashedUrls.Contains(serverUrl)) {
                    serverUrl = null;
                    return false;
                }
                return true;
            }

            // Copy nameServersUrls to other list so that if some server
            // is unavailable, we can receive the failure event concurrently
            // without modifying nameServersUrls
            foreach (string nameServerUrl in nameServersUrls.ToList()) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (GrpcMessageLayer.Instance.Lookup(nameServerUrl, serverId, out serverUrl)) {
                    knownServers.Add(serverId, serverUrl);
                    break;
                }
            }

            return (serverUrl != null);
        }

        public bool LookupMaster(string partitionId, out string masterUrl) {
            if (knownMasters.TryGetValue(partitionId, out masterUrl)) {
                // Check if crashed
                if (crashedUrls.Contains(masterUrl)) {
                    masterUrl = null;
                    return false;
                }
                return true;
            }

            foreach (string nameServerUrl in nameServersUrls.ToList()) {
                if (GrpcMessageLayer.Instance.LookupMaster(nameServerUrl, partitionId, out masterUrl)) {
                    knownMasters.Add(partitionId, masterUrl);
                    break;
                }
            }

            return (masterUrl != null);
        }

        private void AddCrashed(object sender, ReplicaFailureEventArgs args) {
            nameServersUrls.Remove(args.Url);
            crashedUrls.Add(args.Url);
        }
    }
}
