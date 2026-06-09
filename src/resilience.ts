// Turn resilience: every turn runs under a deadline + bounded retry with exponential backoff + jitter.
// Turn-completion degrades under sustained burst/throttle on the BYOK model, so a non-completing turn is
// retried, not surfaced as a failure. Composed from cockatiel; no hand-rolled backoff.
import { retry, timeout, wrap, handleAll, ExponentialBackoff, TimeoutStrategy, type IPolicy } from "cockatiel";

export interface ResilienceOptions {
  /** Hard per-attempt deadline in ms. */
  readonly attemptTimeoutMs: number;
  /** Max attempts including the first. */
  readonly maxAttempts: number;
  /** Initial backoff in ms (doubles per attempt, jittered). */
  readonly initialBackoffMs: number;
  /** Cap on backoff in ms. */
  readonly maxBackoffMs: number;
}

export const DEFAULT_RESILIENCE: ResilienceOptions = {
  attemptTimeoutMs: 180_000,
  maxAttempts: 4,
  initialBackoffMs: 1_000,
  maxBackoffMs: 30_000,
};

/** Build a timeout→retry policy. Each attempt is deadline-bounded; failures/timeouts retry with backoff. */
export function buildTurnPolicy(opts: ResilienceOptions = DEFAULT_RESILIENCE): IPolicy {
  const perAttemptTimeout = timeout(opts.attemptTimeoutMs, TimeoutStrategy.Aggressive);
  const withRetry = retry(handleAll, {
    maxAttempts: opts.maxAttempts - 1,
    backoff: new ExponentialBackoff({ initialDelay: opts.initialBackoffMs, maxDelay: opts.maxBackoffMs }),
  });
  // retry wraps timeout: a timed-out attempt becomes a retryable failure.
  return wrap(withRetry, perAttemptTimeout);
}

/** Run an async unit of work under the turn policy. Throws once attempts are exhausted. */
export function runResilient<T>(work: (signal: AbortSignal) => Promise<T>, opts?: ResilienceOptions): Promise<T> {
  return buildTurnPolicy(opts).execute(({ signal }) => work(signal));
}
