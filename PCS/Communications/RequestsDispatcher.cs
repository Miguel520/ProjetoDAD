
using System.Diagnostics;
using System;

namespace PCS.Communications {
    class RequestsDispatcher {
        public RequestsDispatcher() {
        }

        public static bool CreateServerProcess(CreateServerArguments args) {
            Console.WriteLine(
                "[{0}] Starting Server {1} in port {2} with the id {3}",
                DateTime.Now.ToString("HH:mm:ss"),
                args.Host,
                args.Port.ToString(),
                args.ServerId.ToString());

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
            Console.WriteLine(
                "[{0}] Starting Client {1} in URL {2}:{3}",
                DateTime.Now.ToString("HH:mm:ss"),
                args.Username, 
                args.Host, 
                args.Port);

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
