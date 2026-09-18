package io.kestra.plugin.dropbox;

import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;

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

    @Override
    public void kill() {
        this.isCancelled.set(true);
    }

    @Override
    public void stop() {
        this.kill();
    }

    protected void throwIfCancelled(String message) {
        if (this.isCancelled.get()) {
            throw new KilledException(message);
        }
    }
}
