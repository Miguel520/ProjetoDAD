
using System;
using System.Threading;
using Grpc.Core;
using Client.Configuration;
using ProtoClientConfiguration = Common.Protos.ClientConfiguration.ClientConfigurationService;
using Client.Communications;
using Client.Grpc;

using Client.Naming;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Client {
    class Program {
        static void Main(string[] args) {

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);

            ClientController controller = new ClientController();
            ClientConfiguration clientConfig = ParseArgs(args);

            NamingService namingService = new NamingService(clientConfig.NamingServersUrls);

            //TODO add file read

            RequestsDispatcher dispatcher = new RequestsDispatcher(clientConfig);

            Server client = new Server {
                Services = {
                    ProtoClientConfiguration.BindService(
                        new ConfigurationService(dispatcher))
                },
                Ports = {
                    new ServerPort(
                        clientConfig.Host,
                        clientConfig.Port,
                        ServerCredentials.Insecure)
                }
            };

            client.Start();
            Console.WriteLine(
               $"Client with username {clientConfig.Username} " +
               $"is running at {clientConfig.Url} " +
               $"with script called {clientConfig.Script}");
            Console.WriteLine("Press any key to stop the client...");
            Console.ReadKey();

            client.ShutdownAsync().Wait();

        }

        private static ClientConfiguration ParseArgs(string[] args) {
            if (args.Length < 5 
                || !int.TryParse(args[2], out int port)) {
                OnInvalidArguments();
                Environment.Exit(1);
                return null;
            }

            string username = args[0];
            string host = args[1];
            string script = args[3];

            // All the arguments after script are name servers the client can use
            ImmutableList<string>.Builder builder = ImmutableList.CreateBuilder<string>();

            for (int i = 4; i < args.Length; i++) {
                builder.Add(args[i]);
            }

            return new ClientConfiguration(
                username,
                host,
                port,
                script,
                builder.ToImmutable());
        }

        private static void OnInvalidArguments() {
            Console.Error.WriteLine("Invalid Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Client username host port script_file_name server_host_name server_port_name");
        }
    }
}
