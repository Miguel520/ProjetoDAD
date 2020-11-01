using Client.Commands;
using Client.KVStoreServer;
using Client.Naming;
using Common.Protos.KeyValueStore;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
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
            insideLoop = false;
            numReps = 0;
            for (int i = 0; i < numReps; i++) {
                loopCommands.ForEach(command => command.Accept(this));
            }
        }

        public void OnListGlobalCommand(ListGlobalCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            // Sort partitions by name for better display
            foreach ((string name, ImmutableHashSet<int> serversIds) in
                namingService.Partitions.OrderBy(pair => pair.Key)) {

                bool partitionPrinted = false;
                Random rng = new Random();
                // Randomly iterate set values to distribute load ammong servers
                foreach (int serverId in serversIds.OrderBy(id => rng.Next())) {
                    if (!namingService.Lookup(serverId, out string url)) {
                        continue;
                    }

                    KVStoreConnection connection = new KVStoreConnection(url);

                    if (connection.ListIds(out ImmutableList<Identifier> objectsIds, name)) {
                        foreach (Identifier objectId in
                            objectsIds.OrderBy(objectId => objectId.ObjectId)) {

                            Console.WriteLine(
                                "{0}:{1}",
                                objectId.PartitionName,
                                objectId.ObjectId);

                        }
                        partitionPrinted = true;
                        break;
                    }
                }

                if (!partitionPrinted) {
                    Console.WriteLine(
                        "Unable to print partition {0}: Servers are not responding",
                        name);
                }
            }
        }

        public void OnListServerCommand(ListServerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            if (!namingService.Lookup(command.serverId, out string url)) {
                Console.WriteLine("Error: server id {0} cannot be found", command.serverId);
                return;
            }

            KVStoreConnection connection = new KVStoreConnection(url);

            bool success = connection.ListServer(
                command.serverId,
                out ImmutableList<StoredObject> storedObjects);

            if (!success) {
                Console.WriteLine("Error listing server id {0} ", command.serverId);
                return;
            }

            foreach(StoredObject obj in storedObjects) {
                Console.WriteLine($"Object id: {obj.ObjectId}" +
                    $" {(obj.IsLocked ? "is Locked" : $" has the value {obj.Value}")} " +
                    $" from partition  {obj.PartitionName}" +
                    $" and server {command.serverId}" +
                    $" {(obj.IsMaster ? "is" : "is not")} the master of this partition.");
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
