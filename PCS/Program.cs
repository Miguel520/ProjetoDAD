using System;
using System.IO;
using Grpc.Core;

using PCS.Configuration;
using PCS.Grpc;

using ProtoProcessCreation = Common.Protos.ProcessCreation.ProcessCreationService;

namespace PCS {
    class Program {
        private static readonly int PORT = 10000;
        static void Main(string[] args) {
            PCSConfiguration pcsConfig = ParseArgs(args);

            Server pcsServer = new Server {
                Services = {
                    ProtoProcessCreation.BindService(
                        new ProcessCreation())
                },
                Ports = {
                    new ServerPort(
                        pcsConfig.Host,
                        pcsConfig.Port,
                        ServerCredentials.Insecure)
                }
            };

            try {
                pcsServer.Start();

            } catch (IOException) {
                //IOException is thrown when there is a port that wasn't 
                //bound successfully, which is crucial in this case
                //but server starts anyway

                Console.Error.WriteLine($"The port {pcsConfig.Port} couldn't bind.");
                pcsServer.ShutdownAsync().Wait();
                Environment.Exit(1);
            }

            Console.WriteLine(
                $"PCS server started at {pcsConfig.Url}");
            Console.WriteLine("Press any key to stop the PCS server...");
            Console.ReadKey();

            pcsServer.ShutdownAsync().Wait();
         }
                  
                  
        private static PCSConfiguration ParseArgs(string[] args) {
            if (args.Length != 1) {
                OnInvalidNumberOfArguments();
                Environment.Exit(1);
                return null;
            }
             
            string host = args[0];
            return new PCSConfiguration(
                host,
                PORT);
        }

        private static void OnInvalidNumberOfArguments() {
            Console.Error.WriteLine("Invalid Number of Arguments");
            DisplayUsage();
        }

        private static void DisplayUsage() {
            Console.WriteLine("Usage: PCS hostname");
        }
    }

}
