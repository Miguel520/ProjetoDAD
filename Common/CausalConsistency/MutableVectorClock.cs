using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace Common.CausalConsistency {
    public class MutableVectorClock : VectorClock {

        private readonly ConcurrentDictionary<string, int> vc;
        // Cache for the immutable this
        private ImmutableVectorClock immutableThis = null;

        private MutableVectorClock(ConcurrentDictionary<string, int> vc) {
            this.vc = vc;
        }

        public static MutableVectorClock Empty() {
            return new MutableVectorClock(new ConcurrentDictionary<string, int>());
        }

        public static MutableVectorClock CopyOf(VectorClock other) {
            return new MutableVectorClock(new ConcurrentDictionary<string, int>(other.Clocks));
        }

        public override int this[string key] {
            get {
                if (vc.TryGetValue(key, out int clock)) {
                    return clock;
                }
                return 0;
            }
        }

        public override ImmutableHashSet<string> Ids {
            get {
                lock (this) {
                    return vc.Keys.ToImmutableHashSet();
                }
            }
        }

        public override ImmutableDictionary<string, int> Clocks {
            get {
                lock (this) {
                    return vc.ToImmutableDictionary();
                }
            }
        }

        public void Increment(string serverId) {
            lock (this) {
                vc.AddOrUpdate(serverId, 1, (key, prev) => prev + 1);
                // Invalidate immutable this
                immutableThis = null;
            }
        }

        public void Merge(VectorClock other) {
            // Create immutable other to protect from concurrent accesses
            ImmutableVectorClock immutableOther = other.ToImmutable();

            ImmutableHashSet<string> secondIds = immutableOther.Ids;

            // Dont let concurrent updates while merging
            lock (this) {
                // Get all possible ids
                HashSet<string> allIds = new HashSet<string>(vc.Keys);
                allIds.UnionWith(secondIds);

                foreach (string id in allIds) {
                    int otherClock = immutableOther[id];

                    // Add other value if not there or
                    // update with max between both values
                    vc.AddOrUpdate(id, otherClock, (key, prev) => Math.Max(prev, otherClock));
                }
                // Invalidate immutable this
                immutableThis = null;
            }
        }
        public override ImmutableVectorClock ToImmutable() {
            lock (this) {
                if (immutableThis == null) {
                    immutableThis = ImmutableVectorClock.CopyOf(this);
                }
            }
            return immutableThis;
        }
    }
}
