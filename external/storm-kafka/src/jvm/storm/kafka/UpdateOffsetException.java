package storm.kafka;

public class UpdateOffsetException extends FailedFetchException {

    public UpdateOffsetException(String message) {
        super(message);
    }
}
