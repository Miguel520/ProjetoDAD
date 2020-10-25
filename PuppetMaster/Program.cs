using System;
using System.IO;
using Grpc.Core;
using PuppetMaster.Commands;
using PuppetMaster.Configuration;
using PuppetMaster.NameService;
using static PuppetMaster.Commands.CommandParser;

using NamingServiceProto = Common.Protos.NamingService.NamingService;

namespace PuppetMaster {

    class Program {

        static void Main(string[] args) {

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);

            PMConfiguration config = ParseArgs(args);
            NameServiceDB nameServiceDB = new NameServiceDB();
            PMController controller = new PMController(config, nameServiceDB);
            Server server = new Server {
                Services = {
                    NamingServiceProto.BindService(new NamingService(nameServiceDB))
                },
                Ports = {
                    new ServerPort(
                        config.Host, 
                        config.Port, 
                        ServerCredentials.Insecure)
                }
            };
            server.Start();
            using TextReader textReader = config.InputSource;
            string line;
            ICommand command;
            while ((line = textReader.ReadLine()) != null) {
                if (!TryParse(line, out command)) {
                    Console.WriteLine("Invalid Command");
                    continue;
                }
                command.Accept(controller);
            }
            server.ShutdownAsync().Wait();
        }

        private static PMConfiguration ParseArgs(string[] args) {
            if (args.Length < 2
                || !int.TryParse(args[1], out int port)) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            
            PMConfiguration config = new PMConfiguration {
                Host = args[0],
                Port = port
            };
            switch (args.Length) {
                // Read commands from Standard Input
                case 2:
                    config.InputSource = GetStdinStreamInput();
                    return config;
                // Read commands from Configuration File
                case 3:
                    config.InputSource = GetFileStreamInput(args[0]);
                    return config;
                default:
                    OnInvalidNumberOfArguments();
                    Environment.Exit(1);
                    return null;
            }
        }

        private static TextReader GetStdinStreamInput() {
            Console.WriteLine("Reading Configuration from Standard Input");
            return Console.In;
        }

        private static TextReader GetFileStreamInput(string fileName) {
            Console.WriteLine("Reading Configuration from {0}", fileName);
            try {
                return new StreamReader(fileName);
            }
            catch (FileNotFoundException) {
                Console.Error.WriteLine("{0}: File not found", fileName);
                Environment.Exit(1);
                return null;
            }
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: PuppetMaster name_server_host name_server_port [fileName]");
        }
    }
}
