using Client.Grpc.Simple;
using Client.Naming;
using Common.Utils;
using System.Collections.Immutable;

namespace Client.KVS {
    public class SimpleKVSMessageLayer {

        private static SimpleKVSMessageLayer instance = null;
        private static readonly object instanceLock = new object();
        
        private readonly NamingService namingService = null;

        // Id of the last attachment
        private string attachedId = null;

        private SimpleKVSMessageLayer(NamingService namingService) {
            this.namingService = namingService;
        }

        public static void SetContext(NamingService namingService) {
            lock(instanceLock) {
                Conditions.AssertArgument(namingService != null);
                Conditions.AssertState(instance == null);
                instance = new SimpleKVSMessageLayer(namingService);
            }
        }

        public static SimpleKVSMessageLayer Instance {
            get {
                lock(instanceLock) {
                    Conditions.AssertState(instance != null);
                    return instance;
                }
            }
        }

        public bool Read(
            string partitionId, 
            string objectId,
            out string value) {

            return ReadAttached(partitionId, objectId, out value)
                || ResolveRead(partitionId, objectId, out value);
        }

        public bool Write(
            string partitionId,
            string objectId,
            string value) {

            return namingService.LookupMaster(partitionId, out string masterUrl)
                && SimpleGrpcMessageLayer.Instance.Write(
                    masterUrl,
                    partitionId,
                    objectId,
                    value);
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

            foreach (string serverId in partitionServerIds) {
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

            bool success = SimpleGrpcMessageLayer.Instance.Read(
                        serverUrl,
                        partitionId,
                        objectId,
                        out value);

            if (success) {
                attachedId = serverId;
                return true;
            }
            return false;
        }
    }
}
