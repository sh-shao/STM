# STM

A simple Software Transactional Memory for Java. It implements the TL2 algorithm and supports `retry` / `orElse` / `afterCommit`.

## Usage

```java
AtomicStamp<Integer> a = new AtomicStamp<>(100);
AtomicStamp<Integer> b = new AtomicStamp<>(0);

// Transfer: either both writes take effect or neither does; conflicts are retried automatically
Transaction.atomic(() -> {
    a.put(a.get() - 10);
    b.put(b.get() + 10);
    Transaction.afterCommit(() -> System.out.println("transfer done")); // runs once, after commit
});

// Read-only transaction with a result: sees a consistent snapshot (takes no locks)
int total = Transaction.atomic(() -> a.get() + b.get());

// retry: block while a condition does not hold, until a variable that was read is modified
int x = Transaction.atomic(() -> {
    if (a.get() < 50) Transaction.retry();
    return a.get();
});

// orElse: if the first branch retries, undo its writes and run the second one
int v = Transaction.orElse(() -> q1.take(), () -> q2.take());
```

`Test05` contains `TQueue`, a bounded blocking queue built only on `retry`, with no `wait/notify`.

## How it works (TL2)

- **Global version clock** (a long): incremented by every committing write transaction; each transaction records its snapshot version `readVersion` when it starts.
- **One versioned lock per variable**: a variable's state is an immutable `(value, version, lock owner)`, locked or written back with a single CAS.
- **Read**: if the variable is locked or its version is > readVersion, abort and retry, so a transaction always sees a consistent snapshot.
- **Write**: goes only into the transaction's private write set, invisible to other threads until commit.
- **Commit**: read-only transactions succeed immediately. Write transactions: CAS-lock the write set (abort if a lock is taken, so no deadlock) -> `wv = ++clock` -> validate the read set -> write back and unlock with wv. Transactions that write disjoint variables commit fully in parallel.
- **Rollback**: just discard the write set. An application exception thrown from the body also rolls back, and the exception propagates unchanged.
- **retry**: registers a waiter on every variable in the read set; committers wake waiters after writing. Registering before checking versions means no wake-up is lost. Waiting is interruptible (throws `RetryInterruptedException`).
- **orElse**: checkpoints the write set and the afterCommit list before a branch and restores them if the branch retries.
- **Nesting**: flattened; an inner `atomic` joins the outer transaction.

## Running the tests

```bash
javac -encoding UTF-8 -d bin $(find src -name '*.java')
java -cp bin com.dsc.test.Test03   # two-variable consistency
java -cp bin com.dsc.test.Test04   # counter, bank transfers, rollback, nesting
java -cp bin com.dsc.test.Test05   # retry/orElse/afterCommit, parallel commits
```

## Notes

- A transaction body may run more than once: put irreversible side effects such as I/O in `afterCommit`.
- Values stored in transactional variables should be immutable (or copied before `put`); mutating them in place bypasses the transaction.
- `ConflictException` and the retry signal extend `Error`; do not `catch (Throwable)` inside a transaction body.
