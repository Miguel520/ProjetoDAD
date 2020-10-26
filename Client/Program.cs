
using System;
using System.Threading;
using Grpc.Core;
using Client.Configuration;
using ProtoClientConfiguration = Common.Protos.ClientConfiguration.ClientConfigurationService;
using Client.Communications;
using Client.Grpc;
using static Client.Commands.CommandParser;

using Client.Naming;
using Client.Commands;

namespace Client {
    class Program {
        static void Main(string[] args) {

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);

            ClientController controller = new ClientController();
            ClientConfiguration clientConfig = ParseArgs(args);

            NamingService namingService = new NamingService(
                clientConfig.ServerHost,
                clientConfig.ServerPort);

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

            string[] lines = System.IO.File.ReadAllLines(clientConfig.Script);

            ICommand command;
            foreach (string line in lines) {
                if(!TryParse(line, out command)) {
                    Console.WriteLine("Invalid Command");
                    continue;
                }
                command.Accept(controller);
            }

            client.ShutdownAsync().Wait();

        }

        private static ClientConfiguration ParseArgs(string[] args) {
            if (args.Length != 6 
                || !int.TryParse(args[2], out int port)
                || !int.TryParse(args[5], out int serverPort)) {
                OnInvalidArguments();
                Environment.Exit(1);
                return null;
            }

            string username = args[0];
            string host = args[1];
            string script = args[3];
            string serverHost = args[4];

            return new ClientConfiguration(
                username,
                host,
                port,
                script,
                serverHost,
                serverPort);
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
