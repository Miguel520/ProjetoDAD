using System;

using Server.Configuration;

namespace Server {
    class Program {
        static void Main(string[] args) {
            ServerConfiguration serverConfiguration = ParseArgs(args);
            Console.WriteLine($"Id: {serverConfiguration.ServerId}");
            Console.WriteLine($"Host: {serverConfiguration.Host}");
            Console.WriteLine($"Port: {serverConfiguration.Port}");
            Console.WriteLine($"Url: {serverConfiguration.Url}");
        }

        private static ServerConfiguration ParseArgs(string[] args) {
            if (args.Length != 3
                || !int.TryParse(args[0], out int serverId)
                || !int.TryParse(args[2], out int port)) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
            string host = args[1];
            return new ServerConfiguration(serverId, host, port);
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: Server server_id host port");
        }
    }
}
