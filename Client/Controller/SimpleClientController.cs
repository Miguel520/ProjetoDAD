﻿using Common.Protos.SimpleKeyValueStore;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;

using Client.Commands;
using Client.Naming;
using Client.KVS;
using Client.Grpc.Simple;

namespace Client.Controller {
    public class SimpleClientController : ICommandHandler {

        // Loop variables
        private static readonly string LOOPSTRING = "$i";
        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;
        private int currentRep = -1;
        private readonly NamingService namingService;

        // Current stopwatch
        private Stopwatch stopwatch = null;

        public SimpleClientController(NamingService namingService) {
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

            // Sort partitions by id for better display
            Console.WriteLine(
                "[{0}] List Global System",
                DateTime.Now.ToString("HH:mm:ss"));

            foreach (string serverId in namingService.ServersIds.Sort()) {
                if (!namingService.Lookup(serverId, out string serverUrl)) {
                    continue;
                }
                if (SimpleGrpcMessageLayer.Instance.ListServer(serverUrl, out ImmutableList<StoredObject> storedObjects)) {
                    foreach (StoredObject storedObject in storedObjects) {
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

            bool success = SimpleGrpcMessageLayer.Instance.ListServer(
                url,
                out ImmutableList<StoredObject> storedObjects);

            if (!success) {
                Console.WriteLine(
                    "[{0}] List server failed",
                    DateTime.Now.ToString("HH:mm:ss"));
                return;
            }

            if (storedObjects.Count != 0) {
                foreach (StoredObject obj in storedObjects) {
                    Console.WriteLine("[{0}] Server {1} ({2}), object: <{3},{4}> = '{5}' ({6})",
                        DateTime.Now.ToString("HH:mm:ss"),
                        serverId,
                        obj.IsMaster ? "Master" : "Not Master",
                        obj.PartitionId,
                        obj.ObjectId,
                        obj.Value,
                        obj.IsLocked ? "Locked" : "Unlocked");
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

            bool success;
            string value;
            // No fallback server
            if (serverId == null || serverId == "-1") {
                success = SimpleKVSMessageLayer.Instance.Read(
                    partitionId,
                    objectId,
                    out value);
            }
            else {
                success = SimpleKVSMessageLayer.Instance.ReadFallback(
                    partitionId,
                    objectId,
                    serverId,
                    out value);
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

            if (SimpleKVSMessageLayer.Instance.Write(partitionId, objectId, value)) {
                Console.WriteLine(
                    "[{0}] Write object <{1},{2}> with value {3} successfull",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId,
                    objectId,
                    value);
            }
            else {
                Console.WriteLine(
                    "[{0}] Write object <{1},{2}> failed",
                    DateTime.Now.ToString("HH:mm:ss"),
                    partitionId,
                    objectId);
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
    }
}
