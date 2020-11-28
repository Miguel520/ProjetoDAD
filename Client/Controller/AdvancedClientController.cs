using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading;

using Client.Commands;
using Client.KVS;
using Client.Naming;
using Common.Protos.AdvancedKeyValueStore;

namespace Client.Controller {
    public class AdvancedClientController : ICommandHandler {

        // Loop variables
        private static readonly string LOOPSTRING = "$i";
        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private int currentRep = -1;
        private readonly NamingService namingService;

        public AdvancedClientController(NamingService namingService) {
            this.namingService = namingService;
        }

        public void OnBeginRepeatCommand(BeginRepeatCommand command) {
            insideLoop = true;
            numReps = command.Iterations;
            loopCommands.Clear();
        }

        public void OnEndRepeatCommand(EndRepeatCommand command) {
            insideLoop = false;
            for (int i = 0; i < numReps; i++) {
                currentRep = i + 1;
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

            throw new NotImplementedException();
        }

        public void OnListServerCommand(ListServerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
            string serverId = command.ServerId.Replace(LOOPSTRING, currentRep.ToString());

            if (!AdvancedKVSMessageLayer.Instance.ListServer(
                serverId, 
                out ImmutableList<StoredObject> storedObjects)) {
                
                Console.WriteLine(
                    "[{0}] List server {1} failed",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverId);
                return;
            }

            if (storedObjects.Count != 0) {
                foreach (StoredObject obj in storedObjects) {
                    Console.WriteLine("[{0}] Server {1}, object: <{2},{3}> = '{4}' ({5})",
                        DateTime.Now.ToString("HH:mm:ss"),
                        serverId,
                        obj.PartitionId,
                        obj.ObjectId,
                        obj.ObjectValue,
                        VectorClockToString(obj.ObjectTimestamp));
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
            // Check if server id is null
            string serverId = command.ServerId?.Replace(LOOPSTRING, currentRep.ToString());

            throw new NotImplementedException();
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

            throw new NotImplementedException();
        }

        private string VectorClockToString(VectorClock vectorClock) {
            int numEntries = vectorClock.ServerIds.Count;

            StringBuilder sb = new StringBuilder("[");
            
            for (int i = 0; i < numEntries; i++) {
                sb.Append(vectorClock.ServerIds[i])
                    .Append(": ")
                    .Append(vectorClock.ServerClocks[i])
                    .Append(", ");
            }
            sb.Remove(sb.Length - 3, 2);
            sb.Append("]");

            return sb.ToString();
        }
    }
}

