using Common.Grpc;
using Common.Utils;
using System;

using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Simple;
using KVStoreServer.Replication.Simple;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Replication.Advanced;
using KVStoreServer.Broadcast;

namespace KVStoreServer {
    class Program {
        static void Main(string[] args) {

            AppContext.SetSwitch(
               "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
               true);

            ChannelPool.SetMaxOpenChannels(10);

            ServerConfiguration serverConfig = ParseArgs(args, out bool file);

            switch (serverConfig.Version) {
                case 1:
                    RunSimpleVersion(serverConfig, file);
                    break;
                case 2:
                    RunAdvancedVersion(serverConfig, file);
                    break;
                default:
                    throw new InvalidOperationException("Unknown Version");
            }
        }

        private static void RunSimpleVersion(ServerConfiguration serverConfig, bool file) {
            SimpleGrpcMessageLayer.SetContext(serverConfig);

            // Add self id so that server knows itself
            SimplePartitionsDB partitionsDB = new SimplePartitionsDB(
                serverConfig.ServerId,
                HttpURLs.FromHostAndPort(serverConfig.Host, serverConfig.Port));

            if (file) {
                if (!partitionsDB.ConfigurePartitions(serverConfig.Filename)) {
                    Console.Error.WriteLine("Couldn't configure server from file.");
                    DisplaySimpleFileSyntax();
                }
            }

            ReplicationService service = new ReplicationService(partitionsDB, serverConfig);
            // Bind handlers for messages
            service.Bind();

            FailureDetectionLayer.Instance.Start();

            Console.WriteLine(
                "[{0}] Server {1} running at {2} with simple version",
                DateTime.Now.ToString("HH:mm:ss"),
                serverConfig.ServerId,
                serverConfig.Url);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            FailureDetectionLayer.Instance.Shutdown();
        }

        private static void RunAdvancedVersion(ServerConfiguration serverConfig, bool file) {
            AdvancedGrpcMessageLayer.SetContext(serverConfig);

            AdvancedPartitionsDB partitionsDB = new AdvancedPartitionsDB(
                serverConfig.ServerId,
                HttpURLs.FromHostAndPort(serverConfig.Host, serverConfig.Port));

            if (file) {
                if (!partitionsDB.ConfigurePartitions(serverConfig.Filename)) {
                    Console.Error.WriteLine("Couldn't configure server from file.");
                    DisplayAdvacnedFileSyntax();
                }
            }

            AdvancedReplicationService service = new AdvancedReplicationService(partitionsDB, serverConfig);
            service.Bind();

            ReliableBroadcastLayer.Instance.Start();

            Console.WriteLine(
                "[{0}] Server {1} running at {2} with advanced version",
                DateTime.Now.ToString("HH:mm:ss"),
                serverConfig.ServerId,
                serverConfig.Url);
            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            ReliableBroadcastLayer.Instance.Shutdown();
        }

        private static ServerConfiguration ParseArgs(string[] args, out bool file) {
            file = false;
            string filename = null;
            if ((args.Length != 6 && args.Length != 7)
                || !int.TryParse(args[2], out int port)
                || !int.TryParse(args[3], out int minDelay)
                || !int.TryParse(args[4], out int maxDelay)
                || !int.TryParse(args[5], out int version)) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            string serverId = args[0];
            string host = args[1];

            if (args.Length == 7) { 
                filename = args[6];
                file = true;
            }

            return new ServerConfiguration(
                serverId,
                host,
                port,
                minDelay,
                maxDelay,
                filename,
                version);
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Server server_id host port min_delay max_delay version [config_file]");
        }

        private static void DisplaySimpleFileSyntax() {
            Console.WriteLine("File must be in KVStoreServer/ConfigFiles. Example:");
            Console.WriteLine("Nservers Mpartitions");
            Console.WriteLine("Server_id_1, server_url_1");
            Console.WriteLine("...");
            Console.WriteLine("Server_id_n,server_url_n");
            Console.WriteLine("Partition_id_1,server_master,server_id,server_id,...");
            Console.WriteLine("...");
            Console.WriteLine("Partition_id_m,server_master,server_id,server_id,...");
        }

        private static void DisplayAdvacnedFileSyntax() {
            Console.WriteLine("File must be in KVStoreServer/ConfigFiles. Example:");
            Console.WriteLine("Nservers Mpartitions");
            Console.WriteLine("Server_id_1, server_url_1");
            Console.WriteLine("...");
            Console.WriteLine("Server_id_n,server_url_n");
            Console.WriteLine("Partition_id_1,server_id,server_id,server_id,...");
            Console.WriteLine("...");
            Console.WriteLine("Partition_id_m,server_id,server_id,server_id,...");
        }
    }
}
