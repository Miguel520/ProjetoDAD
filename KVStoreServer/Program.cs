using Grpc.Core;
using System;

using KVStoreServer.Configuration;
using KVStoreServer.Grpc;

using ProtoServerConfiguration = Common.Protos.ServerConfiguration.ServerConfigurationService;
using ProtoKeyValueStore = Common.Protos.KeyValueStore.KeyValueStoreService;

namespace KVStoreServer {
    class Program {
        static void Main(string[] args) {
            ServerConfiguration serverConfig = ParseArgs(args);
            Server server = new Server {
                Services = {
                    ProtoServerConfiguration.BindService(new ConfigurationService()),
                    ProtoKeyValueStore.BindService(new StorageService())
                },
                Ports = {
                    new ServerPort(serverConfig.Host, serverConfig.Port, ServerCredentials.Insecure)
                }
            };
            server.Start();
            Console.WriteLine($"Server with id {serverConfig.ServerId} started at {serverConfig.Url}");
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }

        private static ServerConfiguration ParseArgs(string[] args) {
            if (args.Length != 3
                || !int.TryParse(args[0], out int serverId)
                || !int.TryParse(args[2], out int port)) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            string host = args[1];
            return new ServerConfiguration(serverId, host, port);
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Server server_id host port");
        }
    }
}
