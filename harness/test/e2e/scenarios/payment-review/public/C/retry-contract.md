# Retry contract

- A timeout may cause the consumer to repeat a request with the same key.
- All successful repeats must return the original charge identity and amount.
- Reusing a key for different payment content must fail closed.
- The key must not appear in logs or durable review output.
