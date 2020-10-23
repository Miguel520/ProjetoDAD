using Common.Protos.ServerConfiguration;
using Common.Utils;
using System;
using System.Linq;
using System.Collections.Generic;

using PuppetMaster.Commands;
using PuppetMaster.PCS;
using PuppetMaster.KVStoreServer;

namespace PuppetMaster {

    /*
     * Class responsible for the general control of the Puppet Master state
     * Processes received commands and applies necessary operations
     */
    public class PMController : ICommandHandler {

        private int replicationFactor = 0;
        private readonly NameService nameService = new NameService();

        public PMController() {
        }

        public void OnReplicationFactorCommand(ReplicationFactorCommand command) {
            // If replicationFactor != 0 than it was already set in previous command
            if (replicationFactor != 0) {
                Console.Error.WriteLine(
                    "Replication Factor already set to {0}",
                    replicationFactor);
                return;
            }

            if (replicationFactor < 0) {
                Console.Error.WriteLine("Replication Factor must be greater than 0");
                return;
            }

            replicationFactor = command.ReplicationFactor;
        }

        public void OnCreateServerCommand(CreateServerCommand command) {
            int serverId = command.ServerId;
            string url = command.URL;

            // URL should be in the form http://<PCS host>:<Server Port>
            if (!HttpURLs.TryParseHost(url, out string PCSHost)) {
                Console.Error.WriteLine("Invalid Host in URL '{0}'", url);
                return;
            }

            // Check if server already exists
            if (!nameService.TryAddServer(serverId, url)) {
                Console.Error.WriteLine("Server with id {0} already exists", serverId);
                return;
            }

            PCSConnection connection = new PCSConnection(PCSHost);
            if (!connection.CreateServer(serverId, command.MinDelay, command.MaxDelay)) {
                // Remove inserted id if operation failed
                nameService.RemoveServer(serverId);
            }
        }

        public void OnCreatePartitionCommand(CreatePartitionCommand command) {
            int numReplicas = command.NumberOfReplicas;
            string partitionName = command.PartitionName;
            int[] serverIds = command.ServerIds;

            // Check if wanted number of replicas matches replication factor
            if (numReplicas != replicationFactor) {
                Console.Error.WriteLine(
                    $"Invalid Number of Servers for Partition: " +
                    $"{replicationFactor} expected");
                return;
            }

            // Number of parsed servers must be the same as replication factor
            if (serverIds.Length != replicationFactor) {
                Console.Error.WriteLine(
                    $"Invalid number of server ids: " +
                    $"{replicationFactor} expected");
                return;
            }

            // Lookup servers urls
            IEnumerable<Tuple<int, string>> servers = serverIds.Select(id => {
                nameService.TryLookupServer(id, out string url);
                return new Tuple<int, string>(id, url);
            });

            // Check if all servers already existed
            if (!servers.All(server => server.Item2 != null)) {
                Console.WriteLine("Unknown server ids");
                return;
            }

            foreach ((int id, string url) in servers) {
                ServerConfigurationConnection connection =
                    new ServerConfigurationConnection(url);
                // FIXME: Revoke already created partitions
                if(!connection.JoinPartition(partitionName, servers, serverIds[0])) {
                    return;
                }
            }
        }

        public void OnCreateClientCommand(CreateClientCommand command) {
            string username = command.Username;
            string url = command.URL;

            // URL should be in the form http://<PCS host>:<Client Port>
            if (!HttpURLs.TryParseHost(url, out string PCSHost)) {
                Console.Error.WriteLine("Invalid Host in URL '{0}'", url);
                return;
            }

            // Check if client already exists
            if (!nameService.TryAddClient(username, url)) {
                Console.Error.WriteLine("Client with username {0} already exists", username);
                return;
            }

            PCSConnection connection = new PCSConnection(PCSHost);
            if (!connection.CreateClient(username, command.ScriptFile)) {
                // Remove inserted username if operation failed
                nameService.RemoveClient(username);
            }
        }

        public void OnStatusCommand(StatusCommand command) {
            throw new NotImplementedException();
        }

        public void OnCrashServerCommand(CrashServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnFreezeServerCommand(FreezeServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnUnfreezeServerCommand(UnfreezeServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnWaitCommand(WaitCommand command) {
            throw new NotImplementedException();
        }
    }
}
