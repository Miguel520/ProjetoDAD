using Client.Grpc.Advanced;
using Client.Naming;
using Common.CausalConsistency;
using Common.Protos.AdvancedKeyValueStore;
using Common.Utils;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Client.KVS {
    class AdvancedKVSMessageLayer {

        private static AdvancedKVSMessageLayer instance = null;
        private static readonly object instanceLock = new object();
        private string attachedId = null;

        private readonly NamingService namingService = null;

        private readonly Dictionary<string, MutableVectorClock> timestamps
            = new Dictionary<string, MutableVectorClock>();

        private AdvancedKVSMessageLayer(NamingService namingService) {
            this.namingService = namingService;
        }

        public static void SetContext(NamingService namingService) {
            lock (instanceLock) {
                Conditions.AssertArgument(namingService != null);
                Conditions.AssertState(instance == null);
                instance = new AdvancedKVSMessageLayer(namingService);
            }
        }

        public static AdvancedKVSMessageLayer Instance {
            get {
                lock (instanceLock) {
                    Conditions.AssertState(instance != null);
                    return instance;
                }
            }
        }

        public bool Write(
            string partitionId,
            string objectId,
            string value) {

            if (!namingService.ListPartition(partitionId, out ImmutableHashSet<string> serverIds)) {
                return false;
            }
            
            if (!timestamps.TryGetValue(partitionId, out MutableVectorClock timestamp)) {
                timestamp = MutableVectorClock.Empty();
                timestamps.Add(partitionId, timestamp);
            }

            
            foreach (string serverId in serverIds) {
                ImmutableVectorClock replicaTimestamp = null;

                bool success = namingService.Lookup(serverId, out string serverUrl)
                    && AdvancedGrpcMessageLayer.Instance.Write(
                        serverUrl, 
                        partitionId, 
                        objectId, 
                        value, 
                        timestamp.ToImmutable(), 
                        out replicaTimestamp);

                if (success) {
                    timestamp.Merge(replicaTimestamp);
                    return true;
                }
            }
            return false;
        }

        public bool Read(
            string partitionId, 
            string objectId,
            out string value) {

            return ReadAttached(partitionId, objectId, out value)
                || ResolveRead(partitionId, objectId, out value);
        }

        public bool ReadFallback(
            string partitionId,
            string objectId,
            string fallbackServerId,
            out string value) {

            return ReadAttached(partitionId, objectId, out value)
                || ReadSpecificServer(partitionId, objectId, fallbackServerId, out value)
                || ResolveRead(partitionId, objectId, out value);
        }

        public bool ListServer(string serverId, out ImmutableList<StoredObject> objects) {
            objects = default;
            return namingService.Lookup(serverId, out string serverUrl)
                && AdvancedGrpcMessageLayer.Instance.ListServer(serverUrl, out objects);
        }

        private bool ReadAttached(
            string partitionId,
            string objectId,
            out string value) {

            value = default;
            if (attachedId != null
                && ReadSpecificServer(partitionId, objectId, attachedId, out value)) {

                return true;
            }
            attachedId = null;
            return false;
        }

        private bool ReadSpecificServer(
            string partitionId,
            string objectId,
            string serverId,
            out string value) {

            value = default;
            return namingService.IsInPartition(partitionId, serverId)
                && namingService.Lookup(serverId, out string serverUrl)
                && ReadAndAttach(
                    partitionId,
                    objectId,
                    serverId,
                    serverUrl,
                    out value);
        }

        private bool ResolveRead(
            string partitionId,
            string objectId,
            out string value) {

            value = default;
            if (!namingService.ListPartition(
                    partitionId,
                    out ImmutableHashSet<string> partitionServerIds)) {

                return false;
            }

            foreach (string serverId in partitionServerIds)
            {
                if (namingService.Lookup(serverId, out string serverUrl)
                    && ReadAndAttach(
                        partitionId,
                        objectId,
                        serverId,
                        serverUrl,
                        out value)) {
                    return true;
                }
            }
            attachedId = null;
            return false;
        }

        private bool ReadAndAttach(
            string partitionId,
            string objectId,
            string serverId,
            string serverUrl,
            out string value) {

            if (!timestamps.TryGetValue(partitionId, out MutableVectorClock timestamp)) {
                timestamp = MutableVectorClock.Empty();
                timestamps.Add(partitionId, timestamp);
            }

            if (AdvancedGrpcMessageLayer.Instance.Read(
                        serverUrl,
                        partitionId,
                        objectId,
                        out value,
                        timestamp.ToImmutable(),
                        out ImmutableVectorClock replicaTimestamp) 
                    && value != null) {
                timestamp.Merge(replicaTimestamp);
                attachedId = serverId;
                return true;
            }

            return false;
        }
    }
}
