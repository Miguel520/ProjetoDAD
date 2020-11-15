using Common.Protos.KeyValueStore;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

using Client.Commands;
using Client.Grpc;
using Client.Naming;

namespace Client {
    public class ClientController : ICommandHandler {

        // Loop variables
        private static readonly string LOOPSTRING = "$i";
        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private int currentRep = -1;
        private readonly NamingService namingService;
        
        private string attachedServerId;
        private string attachedServerUrl;
        private object attachedServerLock = new object();

        public ClientController(NamingService namingService) {

            GrpcMessageLayer.ReplicaFailureEvent += OnReplicaFailureEvent;

            this.namingService = namingService;
            attachedServerUrl = null;
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
            Console.WriteLine(
                "[{0}] List Global System Ids",
                DateTime.Now.ToString("HH:mm:ss"));

            foreach (string serverId in namingService.ServersIds.Sort()) {
                if (!namingService.Lookup(serverId, out string serverUrl)) {
                    continue;
                }
                if (GrpcMessageLayer.Instance.ListServer(serverUrl, out ImmutableList<StoredObject> storedObjects)) {
                    foreach(StoredObject storedObject in storedObjects) {
                        Console.WriteLine(
                            "[{0}] Server: {1}, object: <{2},{3}> = '{4}'",
                            DateTime.Now.ToString("HH:mm:ss"),
                            serverId,
                            storedObject.PartitionId,
                            storedObject.ObjectId,
                            storedObject.Value);
                    }
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
                Console.WriteLine(
                    "[{0}] List server failed: Replica {1} unavailable",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverId);
                return;
            }

            bool success = GrpcMessageLayer.Instance.ListServer(
                url,
                out ImmutableList<StoredObject> storedObjects);

            if (!success) {
                Console.WriteLine(
                    "[{0}] List server failed",
                    DateTime.Now.ToString("HH:mm:ss"));
                return;
            }

            if (storedObjects.Count != 0) {
                foreach(StoredObject obj in storedObjects) {
                    Console.WriteLine($"[{DateTime.Now.ToString("HH:mm:ss")}]" +
                        $" Object id: {obj.ObjectId}" +
                        $" {(obj.IsLocked ? "is Locked" : $" has the value {obj.Value}")} " +
                        $" from partition  {obj.PartitionId}" +
                        $" and server {serverId}" +
                        $" {(obj.IsMaster ? "is" : "is not")} the master of this partition.");
                }
            }
            else {
                Console.WriteLine(
                    "[{0}] Server {1} empty",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverId);
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

            Monitor.Enter(attachedServerLock);
            
            bool attached = (attachedServerUrl != null);

            // If not previously attached create attachement if possible
            if (!attached && serverId != "-1") {
                //create connection
                attached = AttachServer(serverId);
            }

            bool success = false;
            string value = null;
            // May not be attached if no previous connection and -1 was passed
            // or if attached to server id failed
            if (attached) {
                success = GrpcMessageLayer.Instance.Read(
                    attachedServerUrl,
                    partitionId,
                    objectId,
                    out value);
                Monitor.Exit(attachedServerLock);

                // Retry if unsuccess
                // Can only retry if target replica is not previous connection (-1)
                // and is not already in use (attachedServerId)
                if (!success && 
                    serverId != attachedServerId &&
                    serverId != "-1") {

                    lock(attachedServerLock) {
                        if (AttachServer(serverId)) {
                            success = GrpcMessageLayer.Instance.Read(
                                attachedServerUrl,
                                partitionId,
                                objectId,
                                out value);
                        }
                    }
                }
            }
            else {
                Monitor.Exit(attachedServerLock);
            }
            

            if (success) {
                Console.WriteLine(
                    "[{0}] Object <{1},{2}> has value '{3}'",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId,
                    objectId,
                    value);
            }
            else {
                Console.WriteLine(
                    "[{0}] N/A",
                    DateTime.Now.ToString("HH:mm:ss"));
                return;
            }
        }

        public bool AttachServer(string serverId) {
            attachedServerId = serverId;
            if (namingService.Lookup(attachedServerId, out string url)) {
                attachedServerUrl = url;
                return true;
            }
            return false;
        }

        public void OnWaitCommand(WaitCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            string time = command.Time.Replace(LOOPSTRING, currentRep.ToString());

            if (int.TryParse(time, out int sleepTime)) {
                Console.WriteLine(
                    "[{0}] Waiting {1}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    time);
                Thread.Sleep(sleepTime);
            }
            else {
                Console.WriteLine(
                    "[{0}] Error parsing time",
                    DateTime.Now.ToString("HH:mm:ss"));
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
                
                bool success = GrpcMessageLayer.Instance.Write(
                    url,
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
                        "[{0}] Write object <{1},{2}> failed",
                        DateTime.Now.ToString("HH:mm:ss"),
                        partitionId,
                        objectId);
                }
            } else {
                Console.WriteLine(
                    "[{0}] Write object <{1},{2}> failed: Partition master unavailable",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId,
                    objectId);
            }
        }

        private void OnReplicaFailureEvent(object sender, ReplicaFailureEventArgs args) {
            lock(attachedServerLock) {
                if (args.Url == attachedServerUrl) {
                    attachedServerUrl = null;
                }
            }
        }
    }
}
