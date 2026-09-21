package io.kestra.plugin.dropbox;

import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.exceptions.KilledException;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.tasks.Task;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
public abstract class AbstractCancellableTask extends Task implements WorkerJobLifecycle {

    // Never reset in run(): each attempt deserializes a fresh task instance, so a reset would only swallow a kill delivered just before run().
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public void kill() {
        this.isKilled.set(true);
        this.isCancelled.set(true);
    }

    // A listing is re-runnable, so a shutdown still fails the run rather than passing a partial result off as
    // complete. It is not a user kill though, so it must not be reported as one.
    @Override
    public void stop() {
        this.isCancelled.set(true);
    }

    protected void throwIfCancelled(String message) {
        if (!this.isCancelled.get()) {
            return;
        }

        if (this.isKilled.get()) {
            throw new KilledException(message);
        }

        throw new KestraRuntimeException(message + ": the worker is shutting down");
    }
}
