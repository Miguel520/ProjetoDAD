using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace Common.CausalConsistency {

    /*
     * Class to represent a Vector Clock
     * Each Vector Clock is represented by a set of pairs
     * where each pair is definied by the server id and
     * the clock of the server id
     */
    public abstract class VectorClock {

        public static bool HappensBefore(VectorClock first, VectorClock second) {
            // Create immutable other to protect from concurrent accesses
            ImmutableVectorClock immutableFirst = first.ToImmutable();
            ImmutableVectorClock immutableSecond = second.ToImmutable();

            // Get all possible ids
            ImmutableHashSet<string> firstIds = immutableFirst.Ids;
            ImmutableHashSet<string> secondIds = immutableSecond.Ids;

            HashSet<string> allIds = new HashSet<string>(firstIds);
            allIds.UnionWith(secondIds);

            // For all ids, at least one has to be less than
            // other and none can be greater
            bool oneSmaller = false;
            foreach (string id in allIds) {
                int firstClock = immutableFirst[id];
                int secondClock = immutableSecond[id];

                if (firstClock < secondClock) {
                    oneSmaller = true;
                }
                else if (firstClock > secondClock) {
                    return false;
                }
            }
            return oneSmaller;
        }

        public static bool HappensAfter(VectorClock first, VectorClock second) {
            // Create immutable other to protect from concurrent accesses
            ImmutableVectorClock immutableFirst = first.ToImmutable();
            ImmutableVectorClock immutableSecond = second.ToImmutable();

            // Get all possible ids
            ImmutableHashSet<string> firstIds = immutableFirst.Ids;
            ImmutableHashSet<string> secondIds = immutableSecond.Ids;

            HashSet<string> allIds = new HashSet<string>(firstIds);
            allIds.UnionWith(secondIds);

            // For all ids, at least one has to be greater than
            // other and none can be less
            bool oneGreater = false;
            foreach (string id in allIds) {
                int firstClock = immutableFirst[id];
                int secondClock = immutableSecond[id];

                if (firstClock > secondClock) {
                    oneGreater = true;
                }
                else if (firstClock < secondClock) {
                    return false;
                }
            }
            return oneGreater;
        }

        public abstract ImmutableHashSet<string> Ids { get; }

        public abstract ImmutableDictionary<string, int> Clocks { get; }

        public abstract int this[string key] { get; }

        public abstract ImmutableVectorClock ToImmutable();

        public override string ToString() {
            ImmutableDictionary<string, int> vc = Clocks;

            StringBuilder sb = new StringBuilder("[");
            if (!vc.IsEmpty) {
                foreach ((string serverId, int clock) in vc) {
                    sb.Append(serverId)
                        .Append(": ")
                        .Append(clock)
                        .Append(", ");
                }
                sb.Remove(sb.Length - 2, 2);
            }
            else {
                sb.Append(' ');
            }
            sb.Append("]");

            return sb.ToString();
        }
    }
}
