using System;

namespace Common.Exceptions {
    public class ReplicaFailureException : Exception {

        public ReplicaFailureException(string url) {
            Url = url;
        }
        public string Url { get; }
    }
}
