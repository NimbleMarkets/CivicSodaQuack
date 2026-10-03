#!/usr/bin/env bash
# The protocol. Run the batches in order, same MODEL and LABEL for all of them:
#   sessions.sh 1   discovery: does the model write or read notes unprompted?
#   sessions.sh 2   asked to record notes; a fresh session; a planted false note
#   sessions.sh 3   told to read notes first: does it list, and does it trust?
# Batch 2 plants a deliberately FALSE note in the global pad. It is part of the test.
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
run="$EX_DIR/run.sh"

case "${1:-}" in
1)
  "$run" ex-copa-1 "I'm new to the COPA complaints data. Work out what it contains, how complete it is, and what I should watch out for, then show me complaints per year."
  "$run" ex-copa-2 "Which month of the year has the most COPA complaints, historically? Chart it."
  "$run" ex-victims-1 "What is the age and sex breakdown of shooting victims in 2024, and which wards have the most?"
  ;;
2)
  "$run" ex-victims-2 "Explore the victims dataset gumc-mgzr briefly: look at the actual values of the columns you would filter or group on (the gunshot injury flag, age, sex, victimization_primary, day_of_week), the date range, and anything surprising. Record in global notes what a later analyst should know about this dataset before querying it. Then tell me how many victims with a gunshot injury there were in 2024."
  "$run" ex-victims-3 "What is the age and sex breakdown of shooting victims in 2024, and which wards have the most?"
  mkdir -p "$OUT/scratch/global"
  cp "$EX_DIR/planted-note.txt" "$OUT/scratch/global/chicago.copa.notes"
  "$run" ex-copa-3 "How many COPA complaints were filed in 2026, and is that comparable to 2025?"
  ;;
3)
  "$run" ex-copa-4 "First read the global notes about this data. Then: how many COPA complaints were filed in 2026 so far, and is that comparable to the full year 2025?"
  "$run" ex-victims-4 "First read the global notes about this data. Then: how many victims with a gunshot injury were there in 2024?"
  ;;
*) echo "usage: sessions.sh 1|2|3" >&2; exit 2 ;;
esac
