using Common.Grpc;
using Common.Utils;
using Grpc.Core;
using System;

using KVStoreServer.Communications;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc;
using KVStoreServer.Replication;

using ProtoServerConfiguration = Common.Protos.ServerConfiguration.ServerConfigurationService;
using ProtoKeyValueStore = Common.Protos.KeyValueStore.KeyValueStoreService;
using NamingServiceProto = Common.Protos.NamingService.NamingService;
using ReplicationServiceProto = Common.Protos.Replication.ReplicationService;

namespace KVStoreServer {
    class Program {
        static void Main(string[] args) {

            AppContext.SetSwitch(
               "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
               true);

            ChannelPool.SetMaxOpenChannels(10);

            ServerConfiguration serverConfig = ParseArgs(args, out bool file);

            // Add self id so that server knows itself
            PartitionsDB partitionsDB = new PartitionsDB(
                serverConfig.ServerId,
                HttpURLs.FromHostAndPort(serverConfig.Host, serverConfig.Port));

            if (file) {
                if (!partitionsDB.ConfigurePartitions(serverConfig.Filename)) {
                    Console.Error.WriteLine("Couldn't configure server from file.");
                    DisplayFileSyntax();
                }
            }

            ReplicationService replicationService = new ReplicationService(
                partitionsDB,
                serverConfig);

            RequestsDispatcher dispatcher = new RequestsDispatcher(
                serverConfig,
                replicationService,
                partitionsDB);

            Server server = new Server {
                Services = {
                    ProtoServerConfiguration.BindService(
                        new ConfigurationService(dispatcher)),
                    ProtoKeyValueStore.BindService(new StorageService(dispatcher)),
                    NamingServiceProto.BindService(new NamingService(partitionsDB)),
                    ReplicationServiceProto.BindService(new PartitionReplicationService(dispatcher))
                    
                },
                Ports = {
                    new ServerPort(
                        serverConfig.Host,
                        serverConfig.Port,
                        ServerCredentials.Insecure)
                }
            };
            server.Start();
            Console.WriteLine(
                $"Server with id {serverConfig.ServerId} " +
                $"started at {serverConfig.Url}");
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }

        private static ServerConfiguration ParseArgs(string[] args, out bool file) {
            file = false;
            string filename = null;
            if ((args.Length != 5 && args.Length != 6)
                || !int.TryParse(args[2], out int port)
                || !int.TryParse(args[3], out int minDelay)
                || !int.TryParse(args[4], out int maxDelay)) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            string serverId = args[0];
            string host = args[1];

            if (args.Length == 6) { 
                filename = args[5];
                file = true;
            }

            return new ServerConfiguration(
                serverId,
                host,
                port,
                minDelay,
                maxDelay,
                filename
                );
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Server server_id host port min_delay max_delay");
        }

        private static void DisplayFileSyntax() {
            Console.WriteLine("File must be in KVStoreServer/ConfigFiles. Example:");
            Console.WriteLine("Nservers Mpartitions");
            Console.WriteLine("Server_id_1, server_url_1");
            Console.WriteLine("...");
            Console.WriteLine("Server_id_n,server_url_n");
            Console.WriteLine("Partition_id_1,server_master,server_id,server_id,...");
            Console.WriteLine("...");
            Console.WriteLine("Partition_id_m,server_master,server_id,server_id,...");
        }
    }
}
