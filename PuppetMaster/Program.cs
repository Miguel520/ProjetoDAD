using System;
using System.IO;
using PuppetMaster.Commands;
using PuppetMaster.Configuration;
using PuppetMaster.NameService;
using static PuppetMaster.Commands.CommandParser;

namespace PuppetMaster {

    class Program {

        private static readonly int PORT = 10001;

        static void Main(string[] args) {

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                true);

            PMConfiguration config = ParseArgs(args);
            NameServiceDB nameServiceDB = new NameServiceDB();
            PMController controller = new PMController(config, nameServiceDB);

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

            Console.WriteLine(
                "[{0}] Instructions Completed. Press any key to exit...",
                DateTime.Now.ToString("HH:mm:ss"));
            Console.ReadKey();
        }

        private static PMConfiguration ParseArgs(string[] args) {
            if (args.Length < 1) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            
            PMConfiguration config = new PMConfiguration {
                Host = args[0],
                Port = PORT
            };
            switch (args.Length) {
                // Read commands from Standard Input
                case 1:
                    config.InputSource = GetStdinStreamInput();
                    return config;
                // Read commands from Configuration File
                case 2:
                    config.InputSource = GetFileStreamInput(args[1]);
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
            Console.WriteLine(
                "[{0}] Reading Configuration from {1}",
                DateTime.Now.ToString("HH:mm:ss"),
                fileName);

            string rootDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string scriptsDirectory = rootDirectory + "..\\..\\..\\Scripts\\";
            try {
                return new StreamReader(scriptsDirectory + fileName);
            }
            catch (FileNotFoundException) {
                Console.Error.WriteLine("{0}: File not found", scriptsDirectory + fileName);
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
