---
name: add-to-leaderboard
description: Use this skill when the user wants to add a new codec entry to the leaderboard, update leaderboard rankings, or mentions adding someone's compression results.
---

# Add to Leaderboard

This skill helps add new codec entries to the compression-golf leaderboard in README.md.

## When to Use

- User provides a codec name and size in bytes
- User shares benchmark results to add to the leaderboard
- User asks to update the leaderboard with new results

## Leaderboard Location

The leaderboard is in `README.md` under the "Training Dataset Leaderboard" section.

## Leaderboard Format

```markdown
| Rank | Who                                | Size (Bytes) |
|------|------------------------------------|--------------|
| 1    | [CodecName](src/codecname.rs)      | X,XXX,XXX    |
```

## Instructions

1. **Parse the input**: Extract the codec name and size in bytes from the user's message
2. **Determine rank**: Compare the size against existing entries to find the correct position (smaller = better)
3. **Update the table**:
   - Insert the new entry at the correct rank position
   - Shift all lower-ranked entries down by 1
   - Format the size with commas (e.g., 7,564,554)
   - Link to `src/<codecname>.rs` (lowercase)
4. **Preserve formatting**: Keep the existing table alignment and style

## Example

User input:
```
add samsond 7564554 bytes to leaderboard
```

Action: Insert `| 2    | [samsond](src/samsond.rs)          | 7,564,554    |` at rank 2 (if that's where it belongs based on size), shift existing rank 2+ entries down.

## Notes

- Baseline entries (Naive, Zstd) are italicized with `*[Name](path)*`
- User submissions are not italicized
- The Naive baseline has no rank number
