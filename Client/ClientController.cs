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
        private static string LOOPSTRING = "$i";
        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private int currentRep = -1;
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
            for (int i = 0; i < numReps; i++) {
                currentRep = (i+1);
                loopCommands.ForEach(command => command.Accept(this));
            }
            numReps = 0;
            currentRep = -1;
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

            string serverId = command.ServerId.Replace(LOOPSTRING, currentRep.ToString());

            if (!namingService.Lookup(serverId, out string url)) {
                Console.WriteLine("Error: server id {0} cannot be found", serverId);
                return;
            }

            KVStoreConnection connection = new KVStoreConnection(url);

            bool success = connection.ListServer(
                serverId,
                out ImmutableList<StoredObject> storedObjects);

            if (!success) {
                Console.WriteLine("Error listing server id {0} ", serverId);
                return;
            }

            foreach(StoredObject obj in storedObjects) {
                Console.WriteLine($"Object id: {obj.ObjectId}" +
                    $" {(obj.IsLocked ? "is Locked" : $" has the value {obj.Value}")} " +
                    $" from partition  {obj.PartitionId}" +
                    $" and server {serverId}" +
                    $" {(obj.IsMaster ? "is" : "is not")} the master of this partition.");
            }
        }

        public void OnReadCommand(ReadCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            string partitionId = command.PartitionId.Replace(LOOPSTRING, currentRep.ToString());
            string objectId = command.ObjectId.Replace(LOOPSTRING, currentRep.ToString());
            string serverId = command.ServerId.Replace(LOOPSTRING, currentRep.ToString());

            bool attached = false;

            if (attachedConnection == null) {
                //create connection
                AttachServer(serverId);
                attached = true;
            } 
           
            bool success = attachedConnection.Read(
                    partitionId,
                    objectId,
                    out string value);

            if (!success) {
                if (!attached && serverId != "-1") {
                    AttachServer(serverId);

                    success = attachedConnection.Read(
                        partitionId,
                        objectId,
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
                    objectId,
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

            string time = command.Time.Replace(LOOPSTRING, currentRep.ToString());

            try {
                Console.WriteLine("Waiting {0}", time);
                Thread.Sleep(Int32.Parse(time));
            } catch {
                Console.WriteLine("Error parsing wait command.");
            }
        }

        public void OnWriteCommand(WriteCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            string partitionId = command.PartitionId.Replace(LOOPSTRING, currentRep.ToString());
            string objectId = command.ObjectId.Replace(LOOPSTRING, currentRep.ToString());
            string value = command.Value.Replace(LOOPSTRING, currentRep.ToString());

            if (namingService.LookupMaster(
                partitionId,
                out string url)) {

                KVStoreConnection connection =
                    new KVStoreConnection(url);

                bool success = connection.Write(
                    partitionId,
                    objectId,
                    value);

                if (success) {
                    Console.WriteLine(
                        "[{0}] Write object <{1},{2}> with value {3} successfull",
                        DateTime.Now.ToString("HH:mm:ss"),
                        partitionId,
                        objectId,
                        value);
                } else {
                    Console.WriteLine(
                        "Write object {0} was not sucessful",
                        objectId);
                }
            } else {
                Console.WriteLine("Error on writing command: " +
                    "Master doesn't exist.");
            }
            Thread.Sleep(10000);
        }
    }
}
