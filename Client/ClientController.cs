using Client.Commands;
using Client.KVStoreServer;
using Client.Naming;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Client {
    public class ClientController : ICommandHandler {

        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private readonly NamingService namingService;
        private int attachedServer;
        private KVStoreConnection attachedConnection;

        public ClientController(NamingService namingService) {
            this.namingService = namingService;
            attachedConnection = null;
        }

        public void OnBeginRepeatCommand(BeginRepeatCommand command) {
            insideLoop = true;
            numReps = command.x;
            loopCommands.Clear();
        }

        public void OnEndRepeatCommand(EndRepeatCommand command) {
            for (int i = 0; i < numReps; i++) {
                loopCommands.ForEach(command => command.Accept(this));
            }
            insideLoop = false;
            numReps = 0;
        }

        public void OnListGlobalCommand(ListGlobalCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
        }

        public void OnListServerCommand(ListServerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
        }

        public void OnReadCommand(ReadCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            bool attached = false;

            if (attachedConnection == null) {
                //create connection
                AttachServer(command.serverId);
                attached = true;
            } 
           
            bool success = attachedConnection.Read(
                    command.partitionId,
                    command.objectId,
                    out string value);

            if (!success) {
                if (!attached && command.serverId != -1) {
                    AttachServer(command.serverId);

                    success = attachedConnection.Read(
                        command.partitionId,
                        command.objectId,
                        out value);
                }
            }

            //try again
            if (!success) {
                Console.WriteLine("N/A");
                return;
            }   

            Console.WriteLine( 
                    "Received value to object {0} is {1}",
                    command.objectId,
                    value);
        }

        public void AttachServer(int serverId) {
            attachedServer = serverId;
            namingService.Lookup(attachedServer, out string url);
            attachedConnection = new KVStoreConnection(url);
        }


        public void OnWaitCommand(WaitCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
            Thread.Sleep(command.x);
        }

        public void OnWriteCommand(WriteCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            } 
            
            if (namingService.LookupMaster(
                command.partitionId,
                out string url)) {

                Console.WriteLine("URL IS " + url);

                KVStoreConnection connection =
                    new KVStoreConnection(url);

                bool success = connection.Write(
                    command.partitionId,
                    command.objectId,
                    command.value);

                if (success) {
                    Console.WriteLine(
                        "Sucess in writing object {0}",
                        command.objectId);
                } else {
                    Console.WriteLine(
                        "Write object {0} was not sucessful",
                        command.objectId);
                }
            } else {
                Console.WriteLine("Error on writing command: " +
                    "Master doesn't exist.");
            }
        }
    }
}
