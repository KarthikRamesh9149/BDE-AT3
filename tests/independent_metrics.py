"""Independent CSV calculations for comparison with PostgreSQL reporting views."""
import csv
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import re

D = Decimal


def read_rows(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        header = [v.lower() for v in next(reader)]
        return [dict(zip(header, row)) for row in reader]


def key(value):
    return re.sub(r'\s+', ' ', value.strip()).upper()


def mean(values):
    return sum(values, D(0))/len(values) if values else None


def pct(numerator, denominator):
    return D(100)*numerator/denominator if denominator else None


def expected_marts(source, months):
    with (source/'lga/NSW_LGA_CODE.csv').open(encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f); next(reader)
        lgas = {key(name): (code, name.strip()) for code, name in reader}
    with (source/'lga/NSW_LGA_SUBURB.csv').open(encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f); next(reader)
        suburbs = {key(row[1]): lgas[key(row[0])] for row in reader}
    fields = {'dm_listing_neighbourhood': ['listing_neighbourhood'],
              'dm_property_type': ['property_type','room_type','accommodates'],
              'dm_host_neighbourhood': ['host_neighbourhood_lga','host_lga_code','host_location_status']}
    groups = {name: defaultdict(list) for name in fields}
    for month in months:
        file = datetime.strptime(month, '%Y-%m-%d').strftime('%m_%Y.csv')
        for row in read_rows(source/'airbnb'/file):
            host_location = key(row['host_neighbourhood'])
            host_lga = suburbs.get(host_location)
            status = 'Matched' if host_lga else ('Unknown' if not host_location else
                                               'Overseas' if host_location == 'OVERSEAS' else 'Unmatched')
            code, name = host_lga if host_lga else (None, status)
            active = row['has_availability'] == 't'
            price = D(row['price'])
            stays = 30-int(row['availability_30']) if active else 0
            record = dict(host=int(row['host_id']), active=active, price=price,
                          rating=D(row['review_scores_rating']) if row['review_scores_rating'].strip() else None,
                          superhost=row['host_is_superhost'] == 't', stays=stays, revenue=stays*price)
            groups['dm_listing_neighbourhood'][(lgas[key(row['listing_neighbourhood'])][1],month)].append(record)
            groups['dm_property_type'][(row['property_type'].strip(),row['room_type'].strip(),int(row['accommodates']),month)].append(record)
            groups['dm_host_neighbourhood'][(name,code,status,month)].append(record)
    output = {}
    for name, grouped in groups.items():
        if name != 'dm_host_neighbourhood':
            first = {}
            for group in list(grouped):
                first[group[:-1]] = min(first.get(group[:-1],group[-1]),group[-1])
            for group, start in first.items():
                for month in months:
                    if month >= start: grouped.setdefault(group+(month,),[])
        rows = []
        for group, records in grouped.items():
            result = dict(zip(fields[name]+['month_year'],group))
            prices = sorted(r['price'] for r in records if r['active'])
            active, total = len(prices), len(records)
            hosts = len({r['host'] for r in records})
            revenue = sum((r['revenue'] for r in records),D(0))
            result.update(number_of_distinct_hosts=hosts,total_estimated_revenue=revenue,
                          average_estimated_revenue_per_active_listing=revenue/active if active else None)
            if name == 'dm_host_neighbourhood':
                result['estimated_revenue_per_host'] = revenue/hosts
            else:
                index = months.index(group[-1])
                previous = grouped.get(group[:-1]+(months[index-1],)) if index else None
                prior_active = sum(r['active'] for r in previous) if previous is not None else 0
                prior_inactive = len(previous)-prior_active if previous is not None else 0
                result.update(total_listings=total,active_listings=active,inactive_listings=total-active,
                    active_listings_rate=pct(active,total), minimum_price=min(prices) if prices else None,
                    maximum_price=max(prices) if prices else None,
                    median_price=(prices[(active-1)//2]+prices[active//2])/2 if prices else None,
                    average_price=mean(prices),
                    superhost_rate=pct(len({r['host'] for r in records if r['superhost']}),hosts),
                    average_review_scores_rating=mean([r['rating'] for r in records if r['active'] and r['rating'] is not None]),
                    percentage_change_active_listings=pct(active-prior_active,prior_active),
                    percentage_change_inactive_listings=pct(total-active-prior_inactive,prior_inactive),
                    total_number_of_stays=sum(r['stays'] for r in records))
            rows.append(result)
        output[name] = rows
    return fields, output


def verify_marts(conn, source, months):
    fields, expected = expected_marts(source, months)
    checked = 0
    for name, rows in expected.items():
        with conn.cursor() as cursor:
            cursor.execute('select * from gold.'+name)
            columns = [d.name for d in cursor.description]
            actual = [dict(zip(columns,row)) for row in cursor.fetchall()]
        keys = fields[name]+['month_year']
        def identity(row):
            return tuple(str(row[k]) if row[k] is not None else None for k in keys)
        indexed = {identity(row): row for row in actual}
        assert len(indexed) == len(actual) == len(rows), name
        for expected_row in rows:
            actual_row = indexed[identity(expected_row)]
            for column, expected_value in expected_row.items():
                value = actual_row[column]
                if column in keys:
                    assert str(value) == str(expected_value), (name,column,value,expected_value)
                elif expected_value is None:
                    assert value is None, (name,column,value)
                else:
                    # NUMERIC aggregates may have different division scales.
                    assert value is not None and abs(D(value)-D(expected_value)) < D('0.00000001'), (name,column,value,expected_value)
                checked += 1
    return checked
