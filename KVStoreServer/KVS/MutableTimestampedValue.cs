﻿using Common.Utils;
using KVStoreServer.CausalConsistency;

namespace KVStoreServer.KVS {
    public class MutableTimestampedValue {
        
        private MutableVectorClock timestamp = MutableVectorClock.Empty();

        public MutableTimestampedValue() { }

        public string Value { get; private set; } = null;

        public ImmutableVectorClock Timestamp { get { return timestamp.ToImmutable(); } }

        public string LastWriteServerId { get; private set; } = null;

        /*
         * Updates the given value with a request received from a client
         * This should only be called when the current server receives a write request
         * Returns the immutable timestamped value to be propagated to
         * all other servers
         */
        public ImmutableTimestampedValue Update(string newValue, string serverId) {

            Conditions.AssertArgument(serverId != null);

            lock (this) {
                Value = newValue;
                LastWriteServerId = serverId;
                timestamp.Increment(serverId);
                return ImmutableTimestampedValue.BuildFrom(
                    Value,
                    Timestamp,
                    LastWriteServerId);
            }
        }

        /*
         * Merge with other value from external write on the same
         * key while maintaing causal consistency
         */
        public void Merge(ImmutableTimestampedValue other) {

            Conditions.AssertArgument(other != null);

            string otherValue = other.Value;
            ImmutableVectorClock otherTimestamp = other.Timestamp;
            string otherLastWriteServerId = other.LastWriteServerId;

            // Prevent simultaneous updates
            lock(this) {
                // If current timestamp happens before new timestamp then
                // replace value, timestamp and lastWriteServerId
                if (VectorClock.HappensBefore(timestamp, otherTimestamp)) {
                    Value = otherValue;
                    timestamp = MutableVectorClock.CopyOf(otherTimestamp);
                    LastWriteServerId = otherLastWriteServerId;
                }
                // Concurrent writes: merge timestamps and set value to lowest writeServerId
                else if (!VectorClock.HappensAfter(timestamp, otherTimestamp)) {
                    if (Strings.LessThan(otherLastWriteServerId, LastWriteServerId)) {
                        Value = otherValue;
                        LastWriteServerId = otherLastWriteServerId;
                    }
                    // Always merge timestamps
                    timestamp.Merge(otherTimestamp);
                }
            }
        }
    }
}