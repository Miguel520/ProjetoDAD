using System;
using System.Collections.Generic;

namespace Common.CausalConsistency {

    /*
     * Util class for dealing with vector clocks operations
     */
    public class VectorClocks {

        private VectorClocks() {}

        public static ImmutableVectorClock FromIdsAndClocksList(
            IList<string> serverIds, 
            IList<int> serverClocks) {

            IList<KeyValuePair<string, int>> clocks = new List<KeyValuePair<string, int>>();
            for (int i = 0; i < serverIds.Count; i++) {
                clocks.Add(new KeyValuePair<string, int>(
                    serverIds[i],
                    serverClocks[i]
                ));
            }
            return ImmutableVectorClock.FromClocks(clocks);
        }

        public static (IList<string>, IList<int>) ToIdsAndClocksList(
            ImmutableVectorClock vectorClock) {

            IList<string> serverIds = new List<string>();
            IList<int> clocks = new List<int>();
            foreach ((string serverId, int serverClock) in vectorClock.Clocks) {
                serverIds.Add(serverId);
                clocks.Add(serverClock);
            }
            return (serverIds, clocks);
        }
    }
}
