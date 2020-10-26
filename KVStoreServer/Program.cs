using Grpc.Core;
using System;

using KVStoreServer.Communications;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc;
using KVStoreServer.Replication;

using ProtoServerConfiguration = Common.Protos.ServerConfiguration.ServerConfigurationService;
using ProtoKeyValueStore = Common.Protos.KeyValueStore.KeyValueStoreService;
using NamingServiceProto = Common.Protos.NamingService.NamingService;
using Common.Utils;

namespace KVStoreServer {
    class Program {
        static void Main(string[] args) {
            ServerConfiguration serverConfig = ParseArgs(args);

            // Add self id so that server knows itself
            PartitionsDB partitionsDB = new PartitionsDB(
                serverConfig.ServerId,
                HttpURLs.FromHostAndPort(serverConfig.Host, serverConfig.Port));

            RequestsDispatcher dispatcher = new RequestsDispatcher(
                serverConfig,
                partitionsDB);

            Server server = new Server {
                Services = {
                    ProtoServerConfiguration.BindService(
                        new ConfigurationService(dispatcher)),
                    ProtoKeyValueStore.BindService(new StorageService()),
                    NamingServiceProto.BindService(new NamingService(partitionsDB))
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

        private static ServerConfiguration ParseArgs(string[] args) {
            if (args.Length != 5
                || !int.TryParse(args[0], out int serverId)
                || !int.TryParse(args[2], out int port)
                || !int.TryParse(args[3], out int minDelay)
                || !int.TryParse(args[4], out int maxDelay)) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            string host = args[1];
            return new ServerConfiguration(
                serverId,
                host,
                port,
                minDelay,
                maxDelay);
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Server server_id host port min_delay max_delay");
        }
    }
}
