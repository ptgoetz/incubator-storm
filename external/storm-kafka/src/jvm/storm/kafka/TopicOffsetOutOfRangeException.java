package storm.kafka;

public class TopicOffsetOutOfRangeException extends FailedFetchException {

    public TopicOffsetOutOfRangeException(String message) {
        super(message);
    }
}
