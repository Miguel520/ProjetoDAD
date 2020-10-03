﻿namespace PuppetMaster.PCS {

    /*
     * Factory class to build messages related to PCS communication
     */
    public class PCSMessageFactory {
        private PCSMessageFactory() {
        }

        public static ReplicationFactorRequest BuildReplicationFactorRequest(
            int replicationFactor
        ) {
            return new ReplicationFactorRequest { Factor = replicationFactor };
        }

        public static CreateServerRequest BuildCreateServerRequest(
            int serverId, string url, int minDelay, int maxDelay
        ) {
            return new CreateServerRequest {
                ServerId = serverId,
                Url = url,
                MinDelay = minDelay,
                MaxDelay = maxDelay
            };
        }

        public static CreatePartitionRequest BuildCreatePartitionRequest(
            int partitionSize, string partitionName, int[] serverIds
        ) {
            return new CreatePartitionRequest {
                PartitionSize = partitionSize,
                PartitionName = partitionName,
                ServerIds = { serverIds }
            };
        }

        public static CreateClientRequest BuildCreateClientRequest(
            string username, string url, string scriptFile
        ) {
            return new CreateClientRequest {
                Username = username,
                Url = url,
                Script = scriptFile
            };
        }

        public static StatusRequest BuildStatusRequest() {
            return new StatusRequest();
        }
    }
}
