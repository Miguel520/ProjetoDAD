using Grpc.Net.Client;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Common.Grpc {
    /*
     * Class to manage channels across the application
     * Recently used channels are saved in a LRU fashion
     * Channels are too be used by the application by should not be closed
     * The pool will automatically close the channels when they are discarded
     */
    public class ChannelPool {

        private readonly ConcurrentDictionary<string, LinkedListNode<UrlChannelPair>> channels = 
            new ConcurrentDictionary<string, LinkedListNode<UrlChannelPair>>();

        private readonly LinkedList<UrlChannelPair> channelsPairs =
            new LinkedList<UrlChannelPair>();

        private readonly int maxOpenChannels;

        public ChannelPool(int maxOpenChannels) {
            this.maxOpenChannels = maxOpenChannels;
        }

        public SharedChannel ForUrl(string url) {
            // Only one channel can be created at a time to avoid concurrency issues
            lock (this) {
                if (channels.TryGetValue(url, out LinkedListNode<UrlChannelPair> node)) {
                    channelsPairs.Remove(node);
                    channelsPairs.AddLast(node);
                    return node.Value.Channel;
                }
                else {
                    // Channel does not exist.
                    // Create new channel and delete old one if necessary
                    if (channelsPairs.Count == maxOpenChannels) {
                        LinkedListNode<UrlChannelPair> oldestNode = channelsPairs.First;
                        channels.TryRemove(oldestNode.Value.Url, out _);
                    }
                    GrpcChannel innerChannel = GrpcChannel.ForAddress(url);
                    UrlChannelPair pair = new UrlChannelPair {
                        Channel = new SharedChannel(innerChannel),
                        Url = url
                    };
                    LinkedListNode<UrlChannelPair> newNode = new LinkedListNode<UrlChannelPair>(pair);
                    channelsPairs.AddLast(newNode);
                    return pair.Channel;
                }
            }
        }

        class UrlChannelPair {

            public SharedChannel Channel { get; set; }
            public string Url { get; set; }
            
        }
    }
}
