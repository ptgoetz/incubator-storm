package storm.kafka;

public class TopicOffsetOutOfRangeException extends RuntimeException {

    public TopicOffsetOutOfRangeException(String message) {
        super(message);
    }
}
