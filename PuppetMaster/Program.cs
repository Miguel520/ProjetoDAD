using System;
using System.IO;

using PuppetMaster.Commands;
using static PuppetMaster.Commands.CommandParser;

namespace PuppetMaster {

    class Program {

        static void Main(string[] args) {
            PMController controller = new PMController();
            using TextReader textReader = GetInputStream(args);
            string line;
            ICommand command;
            while ((line = textReader.ReadLine()) != null) {
                if (!TryParse(line, out command)) {
                    Console.WriteLine("Invalid Command");
                    continue;
                }
                command.Accept(controller);
            }
        }

        private static TextReader GetInputStream(string[] args) {
            switch (args.Length) {
                // Read commands from Standard Input
                case 0:
                    return GetStdinStreamInput();
                // Read commands from Configuration File
                case 1:
                    return GetFileStreamInput(args[0]);
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
            Console.WriteLine("Usage: PuppetMaster [fileName]");
        }
    }
}
