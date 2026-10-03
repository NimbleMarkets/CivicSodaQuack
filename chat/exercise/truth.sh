#!/usr/bin/env bash
# Print the ground truth the sessions are graded against, straight from DuckDB.
# Needs the duckdb CLI. Does not need a model.
set -eu
EX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${DB:-$EX_DIR/../../.csq/chicago.duckdb}"
duckdb -readonly "$DB" -c "
select 'victims: gunshot flag values' as what; select gunshot_injury_i, count(*) n from chicago.main.gumc_mgzr group by 1 order by 2 desc;
select 'victims: date range and 2024 rows' as what; select min(date) first_date, max(date) last_date, count(*) n, count(*) filter (where year(date)=2024) n2024 from chicago.main.gumc_mgzr;
select 'victims: 2024 gunshot count' as what; select count(*) n from chicago.main.gumc_mgzr where year(date)=2024 and gunshot_injury_i='YES';
select 'victims: 2024 age buckets' as what; select age, count(*) n from chicago.main.gumc_mgzr where year(date)=2024 group by 1 order by 2 desc;
select 'victims: 2024 top wards (gunshot)' as what; select ward, count(*) n from chicago.main.gumc_mgzr where year(date)=2024 and gunshot_injury_i='YES' group by 1 order by 2 desc limit 5;
select 'copa: complaints per year' as what; select year(complaint_date) y, count(*) n from chicago.main.mft5_nfa8 group by 1 order by 1 desc limit 6;
select 'copa: range' as what; select min(complaint_date) first_date, max(complaint_date) last_date from chicago.main.mft5_nfa8;
select 'copa: busiest calendar month (all years)' as what; select monthname(complaint_date) m, count(*) n from chicago.main.mft5_nfa8 group by 1 order by 2 desc limit 3;
" 2>&1 | sed 's/\x1b\[[0-9;]*m//g'
