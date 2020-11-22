using Common.Grpc;
using Grpc.Core;
using System;
using System.Collections.Immutable;

using Client.Communications;
using Client.Configuration;
using Client.Grpc;
using Client.Naming;
using Client.Commands;

using ProtoClientConfiguration = Common.Protos.ClientConfiguration.ClientConfigurationService;

using static Client.Commands.CommandParser;

namespace Client {
    class Program {
        static void Main(string[] args) {

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);

            ChannelPool.SetMaxOpenChannels(3);

            ClientConfiguration clientConfig = ParseArgs(args);
            
            Console.WriteLine(
                "[{0}] Client {1} executing script {2} at {3} with version {4}",
                DateTime.Now.ToString("HH:mm:ss"),
                clientConfig.Username,
                clientConfig.Script,
                clientConfig.Url,
                clientConfig.Version);

            NamingService namingService = new NamingService(clientConfig.NamingServersUrls);
            ClientController controller = new ClientController(namingService);

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

            string rootDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string scriptsDirectory = rootDirectory + "..\\..\\..\\Scripts\\";

            string[] lines = System.IO.File.ReadAllLines(scriptsDirectory + clientConfig.Script);

            ICommand command;
            foreach (string line in lines) {
                if(!TryParse(line, out command)) {
                    Console.WriteLine("Invalid Command");
                    continue;
                }
                command.Accept(controller);
            }

            Console.WriteLine("Press any key to stop the client...");
            Console.ReadKey();

            client.ShutdownAsync().Wait();
        }

        private static ClientConfiguration ParseArgs(string[] args) {
            if (args.Length < 6 
                || !int.TryParse(args[2], out int port)
                || !int.TryParse(args[4], out int version)) {
                OnInvalidArguments();
                Environment.Exit(1);
                return null;
            }

            string username = args[0];
            string host = args[1];
            string script = args[3];

            // All the arguments after script are name servers the client can use
            ImmutableList<string>.Builder builder = ImmutableList.CreateBuilder<string>();

            for (int i = 5; i < args.Length; i++) {
                builder.Add(args[i]);
            }

            return new ClientConfiguration(
                username,
                host,
                port,
                script,
                builder.ToImmutable(),
                version);
        }

        private static void OnInvalidArguments() {
            Console.Error.WriteLine("Invalid Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Client username host port script version name_server_1_url [name_server_2_url...]");
        }
    }
}
