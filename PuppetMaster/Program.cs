using System;
using System.IO;

namespace PuppetMaster {

    class Program {

        static void Main(string[] args) {
            using(TextReader textReader = GetInputStream(args)) {
                string line;
                while ((line = textReader.ReadLine()) != null) {
                    Console.WriteLine(line);
                }
            }
        }

        private static TextReader GetInputStream(string[] args) {
            switch (args.Length) {
                // Read commands from Standard Input
                case 1:
                    return GetStdinStreamInput();
                // Read commands from Configuration File
                case 2:
                    return GetFileStreamInput(args[1]);
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
