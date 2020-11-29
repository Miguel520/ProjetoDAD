using Common.CausalConsistency;
using Common.Utils;

namespace KVStoreServer.Storage.Advanced {

    /*
     * Immutable timestamped value to allow for
     * no concurrency access and API usage
     */
    public class ImmutableTimestampedValue {
        private ImmutableTimestampedValue(
            string value,
            ImmutableVectorClock timestamp,
            string lastWriteServerId) {

            Value = value;
            Timestamp = timestamp;
            LastWriteServerId = lastWriteServerId;
        }
        public static ImmutableTimestampedValue BuildFrom(
            string value,
            ImmutableVectorClock timestamp,
            string lastWriteServerId) {

            Conditions.AssertArgument(timestamp != null);

            return new ImmutableTimestampedValue(
                value,
                timestamp,
                lastWriteServerId);
        }

        public string Value { get; } = null;

        public ImmutableVectorClock Timestamp { get; }

        public string LastWriteServerId { get; } = null;
    }
}
