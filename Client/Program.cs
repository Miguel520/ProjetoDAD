using Common.Grpc;
using Grpc.Core;
using System;
using System.Collections.Immutable;

using Client.Communications;
using Client.Configuration;
using Client.Naming;
using Client.Commands;

using ProtoClientConfiguration = Common.Protos.ClientConfiguration.ClientConfigurationService;

using static Client.Commands.CommandParser;
using Client.KVS;
using Client.Grpc.Base;
using Client.Controller;
using Client.Grpc.Simple;
using Client.Grpc.Advanced;

namespace Client {
    class Program {
        static void Main(string[] args) {

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);

            ChannelPool.SetMaxOpenChannels(3);

            ClientConfiguration clientConfig = ParseArgs(args);

            switch (clientConfig.Version) {
                case 1:
                    RunSimpleVersion(clientConfig);
                    break;
                case 2:
                    RunAdvancedVersion(clientConfig);
                    break;
                default:
                    throw new InvalidOperationException("Unknown version");
            }
            
        }

        private static void RunSimpleVersion(ClientConfiguration clientConfig) {
            Console.WriteLine(
                "[{0}] Client {1} executing script {2} at {3} with simple version",
                DateTime.Now.ToString("HH:mm:ss"),
                clientConfig.Username,
                clientConfig.Script,
                clientConfig.Url);

            NamingService namingService = new NamingService(
                clientConfig.NamingServersUrls,
                SimpleGrpcMessageLayer.Instance);

            SimpleKVSMessageLayer.SetContext(namingService);

            SimpleClientController controller = new SimpleClientController(namingService);

            RunMainLoop(controller, clientConfig);
        }

        private static void RunAdvancedVersion(ClientConfiguration clientConfig) {
            Console.WriteLine(
                "[{0}] Client {1} executing script {2} at {3} with advanced version",
                DateTime.Now.ToString("HH:mm:ss"),
                clientConfig.Username,
                clientConfig.Script,
                clientConfig.Url);

            NamingService namingService = new NamingService(
                clientConfig.NamingServersUrls,
                AdvancedGrpcMessageLayer.Instance);

            AdvancedKVSMessageLayer.SetContext(namingService);

            AdvancedClientController controller = new AdvancedClientController(namingService);

            RunMainLoop(controller, clientConfig);
        }

        private static void RunMainLoop(ICommandHandler controller, ClientConfiguration clientConfig) {
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
                if (!TryParse(line, out command)) {
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
