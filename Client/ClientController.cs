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

        // Loop variables
        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private readonly NamingService namingService;
        private string attachedServer;
        private KVStoreConnection attachedConnection;

        public ClientController(NamingService namingService) {
            this.namingService = namingService;
            attachedConnection = null;
        }

        public void OnBeginRepeatCommand(BeginRepeatCommand command) {
            insideLoop = true;
            numReps = command.Iterations;
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

            // Sort partitions by id for better display
            foreach ((string partitionId, ImmutableHashSet<string> serversIds) in
                namingService.Partitions.OrderBy(pair => pair.Key)) {

                bool partitionPrinted = false;
                Random rng = new Random();
                // Randomly iterate set values to distribute load ammong servers
                foreach (string serverId in serversIds.OrderBy(id => rng.Next())) {
                    if (!namingService.Lookup(serverId, out string url)) {
                        continue;
                    }

                    KVStoreConnection connection = new KVStoreConnection(url);

                    if (connection.ListIds(out ImmutableList<Identifier> identifiers, partitionId)) {
                        foreach (Identifier identifier in
                            identifiers.OrderBy(objectId => objectId.ObjectId)) {

                            Console.WriteLine(
                                "{0}:{1}",
                                identifier.PartitionId,
                                identifier.ObjectId);

                        }
                        partitionPrinted = true;
                        break;
                    }
                }

                if (!partitionPrinted) {
                    Console.WriteLine(
                        "[{0}] Unable to print partition {1}: Servers are not responding",
                        DateTime.Now.ToString("HH:mm:ss"),
                        partitionId);
                }
            }
        }

        public void OnListServerCommand(ListServerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            if (!namingService.Lookup(command.ServerId, out string url)) {
                Console.WriteLine("Error: server id {0} cannot be found", command.ServerId);
                return;
            }

            KVStoreConnection connection = new KVStoreConnection(url);

            bool success = connection.ListServer(
                command.ServerId,
                out ImmutableList<StoredObject> storedObjects);

            if (!success) {
                Console.WriteLine("Error listing server id {0} ", command.ServerId);
                return;
            }

            foreach(StoredObject obj in storedObjects) {
                Console.WriteLine($"Object id: {obj.ObjectId}" +
                    $" {(obj.IsLocked ? "is Locked" : $" has the value {obj.Value}")} " +
                    $" from partition  {obj.PartitionId}" +
                    $" and server {command.ServerId}" +
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
                AttachServer(command.ServerId);
                attached = true;
            } 
           
            bool success = attachedConnection.Read(
                    command.PartitionId,
                    command.ObjectId,
                    out string value);

            if (!success) {
                if (!attached && command.ServerId != "-1") {
                    AttachServer(command.ServerId);

                    success = attachedConnection.Read(
                        command.PartitionId,
                        command.ObjectId,
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
                    command.ObjectId,
                    value);
        }

        public void AttachServer(string serverId) {
            attachedServer = serverId;
            namingService.Lookup(attachedServer, out string url);
            attachedConnection = new KVStoreConnection(url);
        }


        public void OnWaitCommand(WaitCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
            Thread.Sleep(command.Time);
        }

        public void OnWriteCommand(WriteCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            } 
            
            if (namingService.LookupMaster(
                command.PartitionId,
                out string url)) {

                KVStoreConnection connection =
                    new KVStoreConnection(url);

                bool success = connection.Write(
                    command.PartitionId,
                    command.ObjectId,
                    command.Value);

                if (success) {
                    Console.WriteLine(
                        "[{0}] Write object <{1},{2}> with value {3} successfull",
                        DateTime.Now.ToString("HH:mm:ss"),
                        command.PartitionId,
                        command.ObjectId,
                        command.Value);
                } else {
                    Console.WriteLine(
                        "Write object {0} was not sucessful",
                        command.ObjectId);
                }
            } else {
                Console.WriteLine("Error on writing command: " +
                    "Master doesn't exist.");
            }
            Thread.Sleep(10000);
        }
    }
}
