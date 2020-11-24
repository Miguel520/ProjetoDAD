using Common.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

using PuppetMaster.Client;
using PuppetMaster.Commands;
using PuppetMaster.KVStoreServer;
using PuppetMaster.NameService;
using PuppetMaster.PCS;
using PuppetMaster.Configuration;
using System.Threading.Tasks;

namespace PuppetMaster {

    /*
     * Class responsible for the general control of the Puppet Master state
     * Processes received commands and applies necessary operations
     */
    public class PMController : ICommandHandler {

        private int replicationFactor = 0;
        private readonly PMConfiguration config;
        private readonly NameServiceDB nameServiceDB;

        private bool partitionsCreated = false;
        private Dictionary<string, string[]> partitions = new Dictionary<string, string[]>();

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
            Console.WriteLine(
                "[{0}] Replication Factor set to {1}",
                DateTime.Now.ToString("HH:mm:ss"),
                replicationFactor);
        }

        public void OnCreateServerCommand(CreateServerCommand command) {
            string serverId = command.ServerId;
            string url = HttpURLs.FromHostAndPort(command.Host, command.Port);

            // Check if server already exists
            if (!nameServiceDB.TryAddServer(serverId, url)) {
                Console.Error.WriteLine("Server with id {0} already exists", serverId);
                return;
            }

            PCSConnection connection = new PCSConnection(command.Host);
            connection.CreateServerAsync(
                serverId,
                command.Port,
                command.MinDelay,
                command.MaxDelay,
                config.Version)
            .ContinueWith(antecedent => {
                if (antecedent.Result) {
                    Console.WriteLine(
                        "[{0}] Server started at {1}:{2}",
                        DateTime.Now.ToString("HH:mm:ss"),
                        command.Host,
                        command.Port);
                }
                else {
                    // Remove inserted id if operation failed
                    nameServiceDB.RemoveServer(serverId);
                }
            });
        }

        public void OnCreatePartitionCommand(CreatePartitionCommand command) {
            int numReplicas = command.NumberOfReplicas;
            string partitionId = command.PartitionId;
            string[] serverIds = command.ServerIds;

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

            // Store partition for later creation
            if (partitions.TryAdd(partitionId, serverIds)) {
                Console.WriteLine(
                    "[{0}] Partition '{1}' submitted",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId);
            }
            else {
                Console.WriteLine(
                    "[{0}] Partition '{1}' already exists",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId);
            }
        }

        public void OnCreateClientCommand(CreateClientCommand command) {
            CreatePartitionsIfRequired();

            string username = command.Username;
            string url = HttpURLs.FromHostAndPort(command.Host, command.Port);

            // Check if client already exists
            if (!nameServiceDB.TryAddClient(username, url)) {
                Console.Error.WriteLine("Client with username {0} already exists", username);
                return;
            }

            PCSConnection connection = new PCSConnection(command.Host);
            connection.CreateClientAsync(
                username,
                command.Port,
                command.ScriptFile,
                nameServiceDB.ListServers(),
                config.Version)
            .ContinueWith(antecedent => {
                if (antecedent.Result) {
                    Console.WriteLine(
                        "[{0}] Client started at {1}:{2}",
                        DateTime.Now.ToString("HH:mm:ss"),
                        command.Host, 
                        command.Port);
                }
                else {
                    // Remove inserted username if operation failed
                    nameServiceDB.RemoveClient(username);
                }
            });
        
        }

        public void OnStatusCommand(StatusCommand command) {
            CreatePartitionsIfRequired();

            ImmutableList<string> serverUrls = nameServiceDB.ListServers();
            ImmutableList<string> clientUrls = nameServiceDB.ListClients();
            
            serverUrls.ForEach(url => {
                ServerConfigurationConnection connection =
                    new ServerConfigurationConnection(url);
                connection.StatusAsync().ContinueWith(antecedent => {
                    if (antecedent.Result) {
                        Console.WriteLine("Status sent to server {0}", url);
                    }
                });
            });

            clientUrls.ForEach(url => {
                ClientConfigurationConnection connection =
                    new ClientConfigurationConnection(url);
                connection.StatusAsync().ContinueWith(antecedent => {
                    if (antecedent.Result) {
                        Console.WriteLine("Status sent to client {0}", url);
                    }
                });
            });
        }

        public void OnCrashServerCommand(CrashServerCommand command) {
            CreatePartitionsIfRequired();

            // Check if server exists
            if (!nameServiceDB.TryLookupServer(command.ServerId, out string url)) {
                Console.Error.WriteLine("Server with id {0} doesn't exist", command.ServerId);
                return;
            }

            ServerConfigurationConnection connection = new ServerConfigurationConnection(url);

            connection.CrashAsync().ContinueWith((antecedent) => {
                Console.WriteLine(
                    "[{0}] Server with id {1} crashed",
                    DateTime.Now.ToString("HH:mm:ss"),
                    command.ServerId);
            });
        }

        public void OnFreezeServerCommand(FreezeServerCommand command) {
            CreatePartitionsIfRequired();

            // Check if server exists
            if (!nameServiceDB.TryLookupServer(command.ServerId, out string url)) {
                Console.Error.WriteLine("Server with id {0} doesn't exist", command.ServerId);
                return;
            }

            ServerConfigurationConnection connection = new ServerConfigurationConnection(url);

            connection.FreezeAsync().ContinueWith((antecedent) => {
                if (antecedent.Result) {
                    Console.WriteLine(
                        "[{0}] Server with id {1} freezed",
                        DateTime.Now.ToString("HH:mm:ss"),
                        command.ServerId);
                }
            });
        }

        public void OnUnfreezeServerCommand(UnfreezeServerCommand command) {
            CreatePartitionsIfRequired();

            // Check if server exists
            if (!nameServiceDB.TryLookupServer(command.ServerId, out string url)) {
                Console.Error.WriteLine("Server with id {0} doesn't exist", command.ServerId);
                return;
            }

            ServerConfigurationConnection connection = new ServerConfigurationConnection(url);

            connection.UnFreezeAsync().ContinueWith((antecedent) => {
                if (antecedent.Result) {
                    Console.WriteLine(
                        "[{0}] Server with id {1} unfreezed",
                        DateTime.Now.ToString("HH:mm:ss"),
                        command.ServerId);
                }
            });
        }

        public void OnWaitCommand(WaitCommand command) {
            Console.WriteLine(
                "[{0}] Waiting {1} ms",
                DateTime.Now.ToString("HH:mm:ss"),
                command.SleepTime);
            Thread.Sleep(command.SleepTime);
        }

        private void CreatePartitionsIfRequired() {
            if (partitionsCreated) return;

            List<Task> joinPartitionTasks = new List<Task>();

            foreach ((string partitionId, string[] serverIds) in partitions) {
                // Lookup servers urls
                IEnumerable<Tuple<string, string>> servers = serverIds.Select(id => {
                    nameServiceDB.TryLookupServer(id, out string url);
                    return new Tuple<string, string>(id, url);
                });

                // Check if all servers already existed
                if (!servers.All(server => server.Item2 != null)) {
                    Console.WriteLine("Unknown server ids");
                    return;
                }

                // Send to all servers, even those not in the partition
                // so that everyone knowns the state of the system
                foreach (string url in nameServiceDB.ListServers()) {
                    ServerConfigurationConnection connection =
                        new ServerConfigurationConnection(url);

                    joinPartitionTasks.Add(
                        connection.JoinPartitionAsync(partitionId, servers, serverIds[0]));
                }

            }

            Task.WaitAll(joinPartitionTasks.ToArray());

            Console.WriteLine(
                "[{0}] Partitions created",
                DateTime.Now.ToString("HH:mm:ss"));

            partitionsCreated = true;
        }
    }
}
