﻿
using System.Diagnostics;
using System;

namespace PCS.Communications {
    class RequestsDispatcher {
        public RequestsDispatcher() {
        }

        public static bool CreateServerProcess(CreateServerArguments args) {
            System.Console.WriteLine("Starting Server {0} in port {1} with the id {2}", 
                   args.Host, args.Port.ToString(),args.ServerId.ToString());

            string rootDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string path = rootDirectory + "..\\..\\..\\..\\KVStoreServer\\bin\\Debug\\netcoreapp3.1\\KVStoreServer.exe";

            ProcessStartInfo startInfo = new ProcessStartInfo(path) {
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = false,
                UseShellExecute = true,
                Arguments = args.CollapseArguments()
            };

            if(Process.Start(startInfo) == null) {
                //TODO throw here an exception
                return false;
            }
            return true;
        }

        public static bool CreateClientProcess(CreateClientArguments args) {
            System.Console.WriteLine("Starting Client {0} in URL {1}:{2}",
                    args.Username, args.Host, args.Port.ToString());
            System.Console.WriteLine("Naming server at {0}:{1}",
                args.ServerHost, args.ServerPort);

            string rootDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string path = rootDirectory + "..\\..\\..\\..\\Client\\bin\\Debug\\netcoreapp3.1\\Client.exe";

            ProcessStartInfo startInfo = new ProcessStartInfo(path) {
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = false,
                UseShellExecute = true,
                Arguments = args.CollapseArguments()
            };

            if (Process.Start(startInfo) == null) {
                //TODO throw here an exception
                return false;
            }
            return true;
        }

    }
}