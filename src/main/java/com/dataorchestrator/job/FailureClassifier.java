package com.dataorchestrator.job;

/**
 * Classifies a {@link Throwable} from the Dataproc client into a retryable
 * {@link FailureType#TRANSIENT} or non-retryable {@link FailureType#PERMANENT} failure.
 *
 * <p>Permanent failures are recognised by walking the cause chain for well-known GCP error
 * classes (invalid argument, permission denied, not found, already exists); everything else
 * (network timeouts, transient API errors) is treated as retryable.</p>
 */
public final class FailureClassifier {

    private FailureClassifier() {
    }

    /** Classifies the given throwable. Never returns null. */
    public static FailureType classify(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            String name = t.getClass().getName();
            if (name.contains("InvalidArgument")
                || name.contains("PermissionDenied")
                || name.contains("NotFound")
                || name.contains("AlreadyExists")
                || name.contains("FailedPrecondition")) {
                return FailureType.PERMANENT;
            }
        }
        return FailureType.TRANSIENT;
    }
}
