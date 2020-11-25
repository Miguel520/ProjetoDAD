namespace KVStoreServer.Broadcast {
    public class MessageId {

        public MessageId(string senderId, int senderCounter) {
            SenderId = senderId;
            SenderCounter = senderCounter;
        }

        public string SenderId { get; }

        public int SenderCounter { get; }

        public override bool Equals(object obj) {
            if (typeof(MessageId).IsInstanceOfType(obj)) {
                MessageId other = (MessageId)obj;
                return SenderId == other.SenderId 
                    && SenderCounter == other.SenderCounter;
            }
            return false;
        }
        public override int GetHashCode() {
            return SenderId.GetHashCode() + SenderCounter.GetHashCode();
        }
    }
}
