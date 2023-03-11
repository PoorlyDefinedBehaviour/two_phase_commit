https://www.researchgate.net/publication/275155037_Two-Phase_Commit

# Running

```
cargo r -- --id 0
cargo r -- --id 1
cargo r -- --id 2

curl localhost:5000 -XPOST <int>
```

# TODO

- Participant needs to check if a transaction has been committed or aborted after not receiving a decision from the manager after some time.

- Handle participant crashes

- Handle manager crashes