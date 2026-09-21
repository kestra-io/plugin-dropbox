package io.kestra.plugin.dropbox;

import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.exceptions.KilledException;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.tasks.Task;

import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
public abstract class AbstractCancellableTask extends Task implements WorkerJobLifecycle {

    // Never reset in run(): each attempt deserializes a fresh task instance, so a reset would only swallow a kill delivered just before run().
    @JsonIgnore
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    @Override
    public void kill() {
        this.isCancelled.set(true);
    }

    // stop() stays the WorkerJobLifecycle no-op. It is the graceful drain signal and does not set killedState,
    // so a task that ends itself there is emitted as a real failure instead of being resubmitted once the
    // grace period expires. A listing is re-runnable, so letting core resubmit it beats failing a deploy.

    protected void throwIfCancelled(String message) {
        if (this.isCancelled.get()) {
            throw new KilledException(message);
        }
    }
}
