1. **PARTIAL-03: Make `produce_batch` async; remove `block_in_place`**
   - File: `src/server/engine.rs`
   - Description: The `produce_batch` function is already async. The `block_in_place` is mentioned in the review, but we need to check if it's there. Oh, `block_in_place` is missing in `produce_batch` according to the grep, wait let me check `src/server/engine.rs` again. Ah, `produce_batch` is already async and uses `await` directly! Oh wait, `handle.block_on` isn't there in `src/server/engine.rs`. Let me check if `produce_batch` in `src/server/handler.rs` is where it blocks. Wait, grep didn't find `block_in_place` at all. So I don't need to fix this, or it's already fixed? Let me re-read `src/server/engine.rs`. Wait, `src/server/engine.rs` around line 280: `let quorum_ok = self.replication.await_isr_quorum(...).await;` It is already async and awaited directly! The review is incorrect or outdated for this version. So we can ignore PARTIAL-03 as it is fixed. Wait, let me check the review document again. The review says "ISR Quorum Uses `block_in_place`". Let's check `src/server/handler.rs` for `block_in_place`.
2. **PARTIAL-01: Remove WAL abstraction**
   - Files: `src/server/partition.rs`, `src/wal/*`, `src/lib.rs`
   - Description: Since the WAL is only buffering in RAM and never flushing to a file, and there is no replay logic, it provides no crash safety but introduces complexity. The review recommends removing it entirely for now. I will remove `WalEngine` from `PartitionManager`, and the `wal` module.
3. **PARTIAL-02 & NEW-02: Fix `find_offset_for_timestamp` to use `TimeIndexSegment` + check active seg**
   - Files: `src/segment/manager.rs`, `src/segment/timeindex.rs`
   - Description: Update `SegmentPair` to include `TimeIndexSegment`. Wire it in `SegmentManager::open` and `SegmentManager::rotate_segment`. Modify `find_offset_for_timestamp` to use `time_index.find_offset_for_timestamp` to locate the exact offset efficiently, and correctly check the active segment if not found in historical.
4. **NEW-03: Fix `delete_topic` flush+error propagation on Windows**
   - File: `src/server/engine.rs`
   - Description: Call `pm.flush()` and drop the partition manager *before* calling `std::fs::remove_dir_all`. Propagate errors from `remove_dir_all` instead of swallowing them.
5. **PARTIAL-05: Validate topic names at wire layer**
   - File: `src/protocol/wire.rs` or `src/server/handler.rs`
   - Description: Move or call `validate_topic_name` at the decode or early handling phase before hitting the engine.
6. **OPEN-CRIT-03 (RACE-01): Move segment I/O to `spawn_blocking` to unblock Tokio threads**
   - File: `src/server/partition.rs`
   - Description: Since `PartitionManager::produce_frame` does file I/O inside a mutex, it blocks the async thread. This is tricky to fix without breaking other things. If the task is non-security issues, I'll focus on these.
7. **Complete pre commit steps**
   - Ensure proper testing, verification, review, and reflection are done.
8. **Submit the change.**
   - Once all tests pass, I will submit the change with a descriptive commit message.
