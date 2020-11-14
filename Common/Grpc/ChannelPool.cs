using Grpc.Core;
using Grpc.Net.Client;
using System;
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

        private static readonly int DEFAULT_SIZE = 10;

        private static ChannelPool instance = null;
        private static object instanceLock = new object();

        private static int maxChannels = DEFAULT_SIZE;

        private readonly ConcurrentDictionary<string, LinkedListNode<UrlChannelPair>> channels = 
            new ConcurrentDictionary<string, LinkedListNode<UrlChannelPair>>();

        private readonly LinkedList<UrlChannelPair> channelsPairs =
            new LinkedList<UrlChannelPair>();

        private readonly int maxOpenChannels;

        public static void SetMaxOpenChannels(int maxChannels) {
            lock(instanceLock) {
                if (instance != null) {
                    throw new InvalidOperationException("Channel Pool already in use");
                }
            }
            ChannelPool.maxChannels = maxChannels;
        }

        public static ChannelPool Instance {
            get {
                lock(instanceLock) {
                    if (instance == null) {
                        instance = new ChannelPool(maxChannels);
                    }
                }
                return instance;
            }
        }

        private ChannelPool(int maxOpenChannels) {
            this.maxOpenChannels = maxOpenChannels;
        }

        /*
         * Returns a channel to use
         * The user must call the clean method when it
         * has finished using the channel
         */
        public ChannelBase ForUrl(string url) {
            // Only one channel can be created at a time to avoid concurrency issues
            lock (this) {
                if (channels.TryGetValue(url, out LinkedListNode<UrlChannelPair> node)) {
                    channelsPairs.Remove(node);
                    channelsPairs.AddLast(node);
                    node.Value.Channel.AddRef();
                    return node.Value.Channel;
                }
                else {
                    // Channel does not exist.
                    // Create new channel and delete old one if necessary
                    if (channelsPairs.Count == maxOpenChannels) {
                        LinkedListNode<UrlChannelPair> oldestNode = channelsPairs.First;
                        // Drop self reference
                        oldestNode.Value.Channel.DropRef();
                        channels.TryRemove(oldestNode.Value.Url, out _);
                        channelsPairs.RemoveFirst();
                    }
                    GrpcChannel innerChannel = GrpcChannel.ForAddress(url);
                    UrlChannelPair pair = new UrlChannelPair {
                        Channel = new SharedChannel(innerChannel),
                        Url = url
                    };
                    LinkedListNode<UrlChannelPair> newNode = new LinkedListNode<UrlChannelPair>(pair);
                    channelsPairs.AddLast(newNode);
                    channels.TryAdd(url, newNode);
                    // Add ref for new client
                    pair.Channel.AddRef();
                    return pair.Channel;
                }
            }
        }

        public void ClearChannel(ChannelBase channel) {
            if (typeof(SharedChannel).IsInstanceOfType(channel)) {
                SharedChannel sharedChannel = (SharedChannel)channel;
                sharedChannel.DropRef();
            }
            else {
                throw new InvalidOperationException(
                    "This channel was not created by the ChannelPool");
            }
        }

        class UrlChannelPair {

            public SharedChannel Channel { get; set; }
            public string Url { get; set; }
            
        }
    }
}
