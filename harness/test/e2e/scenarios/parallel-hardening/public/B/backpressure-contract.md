# Backpressure contract

The upload implementation must not buffer the complete body. Cancellation and reader errors propagate without publishing a partial destination. A caller can bound in-flight memory independently of total object size.
