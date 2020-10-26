using Common.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

using PuppetMaster.Client;
using PuppetMaster.Commands;
using PuppetMaster.KVStoreServer;
using PuppetMaster.NameService;
using PuppetMaster.PCS;
using PuppetMaster.Configuration;

namespace PuppetMaster {

    /*
     * Class responsible for the general control of the Puppet Master state
     * Processes received commands and applies necessary operations
     */
    public class PMController : ICommandHandler {

        private int replicationFactor = 0;
        private readonly PMConfiguration config;
        private readonly NameServiceDB nameServiceDB;

        public PMController(PMConfiguration config, NameServiceDB nameServiceDB) {
            this.config = config;
            this.nameServiceDB = nameServiceDB;
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
            Console.WriteLine($"Replication Factor set to {replicationFactor}");
        }

        public void OnCreateServerCommand(CreateServerCommand command) {
            int serverId = command.ServerId;
            string url = HttpURLs.FromHostAndPort(command.Host, command.Port);

            // Check if server already exists
            if (!nameServiceDB.TryAddServer(serverId, url)) {
                Console.Error.WriteLine("Server with id {0} already exists", serverId);
                return;
            }

            PCSConnection connection = new PCSConnection(command.Host);
            if (!connection.CreateServer(serverId, command.Port, command.MinDelay, command.MaxDelay)) {
                // Remove inserted id if operation failed
                nameServiceDB.RemoveServer(serverId);
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
                nameServiceDB.TryLookupServer(id, out string url);
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

            Console.WriteLine($"Partition '{partitionName}' created");
        }

        public void OnCreateClientCommand(CreateClientCommand command) {
            string username = command.Username;
            string url = HttpURLs.FromHostAndPort(command.Host, command.Port);

            // Check if client already exists
            if (!nameServiceDB.TryAddClient(username, url)) {
                Console.Error.WriteLine("Client with username {0} already exists", username);
                return;
            }

            PCSConnection connection = new PCSConnection(command.Host);
            if (!connection.CreateClient(
                username, 
                command.Port, 
                command.ScriptFile, 
                nameServiceDB.ListServers())) {

                // Remove inserted username if operation failed
                nameServiceDB.RemoveClient(username);
            }
        }

        public void OnStatusCommand(StatusCommand command) {

            ImmutableList<string> serverUrls = nameServiceDB.ListServers();
            ImmutableList<string> clientUrls = nameServiceDB.ListClients();

            serverUrls.ForEach(url => {
                ServerConfigurationConnection connection =
                    new ServerConfigurationConnection(url);
                connection.Status();
            });

            clientUrls.ForEach(url => {
                ClientConfigurationConnection connection =
                    new ClientConfigurationConnection(url);
                connection.Status();
            });
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
