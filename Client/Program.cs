
using System;
using Grpc.Core;

using Client.Configuration;

using ProtoClientConfiguration = Common.Protos.ClientConfiguration.ClientConfigurationService;
using Client.Communications;
using Client.Grpc;

namespace Client
{
    class Program
    {
        static void Main(string[] args)
        {
            ClientController controller = new ClientController();

            ClientConfiguration clientConfig = ParseArgs(args);
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
            if (args.Length != 4 || 
                !int.TryParse(args[2], out int port)) {
                OnInvalidArguments();
                Environment.Exit(1);
                return null;
            }

            string username = args[0];
            string host = args[1];
            string script = args[3];

            return new ClientConfiguration(
                username,
                host,
                port,
                script);
        }

        private static void OnInvalidArguments() {
            Console.Error.WriteLine("Invalid Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Client username host port script_file_name");
        }
    }
}
