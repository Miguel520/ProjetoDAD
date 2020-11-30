using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
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
        private static readonly string NO_FALLBACK_SERVER = "-1";
        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private int currentRep = -1;
        private readonly NamingService namingService;

        // Current stopwatch
        private Stopwatch stopwatch = null;

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

            Console.WriteLine(
                "[{0}] List Global",
                DateTime.Now.ToString("HH:mm:ss"));

            foreach (string serverId in namingService.ServersIds) {
                ListServer(serverId);
            }
        }

        public void OnListServerCommand(ListServerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
            string serverId = command.ServerId.Replace(LOOPSTRING, currentRep.ToString());

            ListServer(serverId);
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
            string value;
            bool success = false;


            if (serverId == NO_FALLBACK_SERVER) {
                success = AdvancedKVSMessageLayer.Instance.Read(
                        partitionId,
                        objectId,
                        out value);
            } else {
                success = AdvancedKVSMessageLayer.Instance.ReadFallback(
                        partitionId,
                        objectId,
                        serverId,
                        out value);
            }

            if (success) {
                Console.WriteLine(
                    "[{0}] Object <{1},{2}> has value '{3}'",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId, objectId, value);
            } else {
                Console.WriteLine(
                    "[{0}] N/A",
                    DateTime.Now.ToString("HH:mm:ss"));
            }
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

            if (AdvancedKVSMessageLayer.Instance.Write(partitionId, objectId, value)) {
                Console.WriteLine(
                    "[{0}] Write object <{1},{2}> with value {3} successfull",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId,
                    objectId,
                    value);
            }
            else {
                Console.WriteLine(
                    "[{0}] Write object <{1},{2}> with value {3} failed",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId,
                    objectId,
                    value);
            }
        }

        public void OnBeginTimerCommand(BeginTimerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            // Already timer running
            if (stopwatch != null) {
                Console.WriteLine(
                    "[{0}] Timer already running",
                    DateTime.Now.ToString("HH:mm:ss"));
            }
            else {
                Console.WriteLine(
                    "[{0}] Timer started",
                    DateTime.Now.ToString("HH:mm:ss"));
                stopwatch = Stopwatch.StartNew();
            }
        }

        public void OnEndTimerCommand(EndTimerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }

            // No timer running
            if (stopwatch == null) {
                Console.WriteLine(
                    "[{0}] No Timer was running",
                    DateTime.Now.ToString("HH:mm:ss"));
            }
            else {
                stopwatch.Stop();
                Console.WriteLine(
                    "[{0}] Timer finished: {1} miliseconds elapsed",
                    DateTime.Now.ToString("HH:mm:ss"),
                    stopwatch.Elapsed);
                // Unset timer
                stopwatch = null;
            }
        }

        private void ListServer(string serverId) {
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

        private string VectorClockToString(VectorClock vectorClock) {
            int numEntries = vectorClock.ServerIds.Count;

            StringBuilder sb = new StringBuilder("[");
            
            for (int i = 0; i < numEntries; i++) {
                sb.Append(vectorClock.ServerIds[i])
                    .Append(": ")
                    .Append(vectorClock.ServerClocks[i])
                    .Append(", ");
            }
            sb.Remove(sb.Length - 2, 2);
            sb.Append("]");

            return sb.ToString();
        }
    }
}

