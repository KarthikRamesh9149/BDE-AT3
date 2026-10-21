"""Twelve-month rehearsal using only the disposable local PostgreSQL fixture."""
from test_part1 import database, query, run_month, SOURCE
from test_part2 import build
from independent_metrics import verify_marts


def test_twelve_monthly_cycles(database, tmp_path):
    conn, _ = database
    months = [f'2020-{m:02d}-01' for m in range(5, 13)] + [
        f'2021-{m:02d}-01' for m in range(1, 5)
    ]
    counts = [37562, 36901, 36057, 31391, 34829, 34276,
              33795, 33871, 33902, 33630, 33229, 32679]
    total = 0
    for index, (month, count) in enumerate(zip(months, counts), 1):
        run_month(f'local-year-{month}')
        build(tmp_path, month, month)
        total += count
        assert query(conn, 'select count(*) from gold.fact_listing_month') == [(total,)]
        assert query(conn, 'select count(*) from pipeline.dbt_completions') == [(index,)]
        assert query(conn, 'select count(*) from gold.dim_month') == [(index,)]
        print(f'Year rehearsal: {month}, total {total}, completed {index}', flush=True)

    assert total == 412122
    assert query(conn, """select count(*) from silver.slv_listings
        where observation_month='2020-07-01' and scraped_date='2020-09-05'""") == [(4780,)]
    print('Full-year independent metric checks:', verify_marts(conn, SOURCE, months), flush=True)
    print('Evidence directory:', tmp_path, flush=True)
