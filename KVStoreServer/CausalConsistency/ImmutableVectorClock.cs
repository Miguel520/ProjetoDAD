using Common.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace KVStoreServer.CausalConsistency {

    /*
     * Immutable Vector Clock Class
     * Allows for faster operations since there is no need for locking
     */
    public sealed class ImmutableVectorClock : VectorClock {

        private ImmutableDictionary<string, int> vc;

        private ImmutableVectorClock(ImmutableDictionary<string, int> vc) {
            this.vc = vc;
        }

        public static ImmutableVectorClock Empty() {
            return new ImmutableVectorClock(ImmutableDictionary.Create<string, int>());
        }

        public static ImmutableVectorClock CopyOf(VectorClock other) {
            Conditions.AssertArgument(other != null);
            return new ImmutableVectorClock(other.Clocks);
        }

        public static ImmutableVectorClock FromClocks(IEnumerable<KeyValuePair<string, int>> clocks) {
            Conditions.AssertArgument(clocks != null);
            return new ImmutableVectorClock(ImmutableDictionary.CreateRange(clocks));
        }

        public override ImmutableHashSet<string> Ids { get { return vc.Keys.ToImmutableHashSet(); } }

        public override ImmutableDictionary<string, int> Clocks { get { return vc; } } 

        public override int this[string key] {
            get => vc.GetValueOrDefault(key, 0);
        }

        public override ImmutableVectorClock ToImmutable() {
            return this;
        }
    }
}
