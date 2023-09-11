#!/usr/bin/env python
import asyncio
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from math import ceil
from pathlib import Path
from typing import Optional

import aiohttp
import pandas
import typer as typer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

USER_ID = os.environ['USER_ID']
USER_HASH = os.environ['USER_HASH']

AUTH_HEADERS = {
    'X-User-id': USER_ID,
    'X-User-hash': USER_HASH
}

logger = logging.getLogger()


async def fetch_json(session, url):
    async with session.get(url, headers=AUTH_HEADERS) as response:
        return await response.json()


async def get_device_list(session):
    logger.info(f'Fetching sensor devices list')
    url = 'https://data.uradmonitor.com/api/v1/devices'
    return await fetch_json(session, url)


def datetime_to_unix_timestamp(datetime_obj):
    return int(datetime.timestamp(datetime_obj))


def unix_timestamp_to_datetime(timestamp):
    return datetime.utcfromtimestamp(timestamp).replace(tzinfo=timezone.utc).astimezone(tz=None)


def seconds_since(timestamp):
    return int((datetime.now().astimezone(tz=None) - timestamp).total_seconds())


def get_last_date_of_month(year: int, month: int) -> datetime:
    """Return the last date of the month.

    Args:
        year (int): Year, i.e. 2022
        month (int): Month, i.e. 1 for January

    Returns:
        date (datetime): Last date of the current month
    """

    if month == 12:
        # Edge case since datetime won't allow month 13 (from below code: month + 1)
        return datetime(year, month, 31).astimezone(tz=None)
    else:
        return datetime(year, month + 1, 1).astimezone(tz=None) + timedelta(days=-1)


def generate_month_start_end_dates(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    assert start <= end, 'Starting date must be earlier or equal to ending date'

    while start <= end:
        month_end = get_last_date_of_month(start.year, start.month)
        if month_end > end:
            month_end = end

        yield start, month_end

        start = month_end + timedelta(days=1)


async def get_sensor_list(session, device_id):
    logger.info(f'Fetching sensor list for device:\t{device_id}')
    url = f'https://data.uradmonitor.com/api/v1/devices/{device_id}'
    return await fetch_json(session, url)


async def get_sensor_data(session, device_id, sensor, start_interval=None, end_interval=None):
    now = datetime.now()
    start_timedelta = timedelta(seconds=start_interval)
    start_date = (now - start_timedelta).isoformat()

    end_date = None
    if end_interval:
        end_timedelta = timedelta(seconds=end_interval)
        end_date = (now - end_timedelta).isoformat()

    logger.info(
        f"Fetching sensor data for device:\t{device_id}\t{sensor}\t{start_interval}\t{end_interval}\t{start_date}\t{end_date}")
    path = '/'.join(str(item) for item in [device_id, sensor, start_interval, end_interval] if item)
    url = f'https://data.uradmonitor.com/api/v1/devices/{path}/'
    return await fetch_json(session, url)


async def fetch_data(start_date, end_date=None):
    if end_date is None:
        end_date = datetime.now().astimezone(tz=None)

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10, force_close=True)) as session:
        all_devices = await get_device_list(session)
        device_sensors = await asyncio.gather(*[
            get_sensor_list(session, device['id'])
            for device in all_devices
        ])

        sensor_device_map = defaultdict(lambda: {'devices': [], 'measure_unit': None, 'name': None})

        for index, device in enumerate(all_devices):
            for sensor, sensor_info in device_sensors[index].items():
                if sensor != 'all':
                    sensor_device_map[sensor]['name'] = sensor_info[0]
                    sensor_device_map[sensor]['measure_unit'] = sensor_info[1]
                    sensor_device_map[sensor]['devices'].append(device)

        for sensor, sensor_info in sensor_device_map.items():
            sensor_data = []
            for start_interval, end_interval in generate_month_start_end_dates(start_date, end_date):
                start_interval = seconds_since(start_interval)

                end_interval = end_interval.replace(hour=23, minute=59, second=59)
                end_interval = seconds_since(end_interval)

                if end_interval <= 0:
                    end_interval = None

                data = await asyncio.gather(*[
                    get_sensor_data(session, device['id'], sensor, start_interval, end_interval)
                    for device in sensor_info['devices']
                ])

                for index, device in enumerate(sensor_info['devices']):
                    if isinstance(data[index], list):
                        for entry in data[index]:
                            entry['device_id'] = device['id']
                    else:
                        data[index] = [{'device_id': device['id']}]

                sensor_data += data

            yield sensor_data, sensor, sensor_info['measure_unit']


def create_chart(df, workbook, sheet_name, sensor_name, measurement_unit):
    # Create a chart object.
    chart = workbook.add_chart({'type': 'column'})
    entries_count = len(df) + 1

    # Configure the series of the chart from the dataframe data.
    chart.add_series({
        # 'name': [sheet_name, *labels],
        'categories': f'={sheet_name}!$A2:$A${entries_count}',
        'values': f'={sheet_name}!$B2:$B${entries_count}',
        # 'gap': 2,
    })

    # Configure the chart axes.
    chart.set_x_axis({'name': 'Date'})
    chart.set_y_axis({'name': f'{sensor_name} - {measurement_unit}', 'major_gridlines': {'visible': False}})

    # Turn off chart legend. It is on by default in Excel.
    chart.set_legend({'position': 'none'})

    return chart


def create_spreadsheet(data, sensor_name, measure_unit, reports_folder_path=Path('reports')):
    logger.info(f'Creating excel report for:\t{sensor_name}')

    data = [entry_data for device_data in data for entry_data in device_data]
    df = pandas.DataFrame.from_dict(data).dropna()

    df['datetime'] = df['time'].apply(lambda x: unix_timestamp_to_datetime(x))
    df['date'] = df['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))

    average_per_day = df.groupby('date')[sensor_name].mean().reset_index()
    average_per_day = average_per_day.set_index('date')
    average_per_day_per_device = df.groupby(['device_id', 'date'])[sensor_name].mean().reset_index()
    average_per_day_per_device = average_per_day_per_device.set_index('date')

    if not reports_folder_path.exists():
        reports_folder_path.mkdir(parents=True)
    elif not reports_folder_path.is_dir():
        raise Exception(f"Location for reports '{reports_folder_path}' is a file! Need a folder")

    report_path = reports_folder_path / f'{sensor_name}_report.xlsx'

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pandas.ExcelWriter(report_path, engine='xlsxwriter')
    average_per_day.to_excel(writer, sheet_name='all')
    chart = create_chart(average_per_day, writer.book, 'all', sensor_name, measure_unit)
    writer.sheets['all'].insert_chart('D5', chart, {'x_scale': 2, 'y_scale': 1})

    for group_name, df_group in average_per_day_per_device.groupby('device_id'):
        df_group[sensor_name].to_excel(writer, sheet_name=group_name)
        chart = create_chart(df_group, writer.book, group_name, sensor_name, measure_unit)
        writer.sheets[group_name].insert_chart('D5', chart, {'x_scale': 2, 'y_scale': 1})

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


async def run_report_generation(start_date, end_date=None):
    start_date = start_date.astimezone(tz=None)
    if end_date is not None:
        end_date = end_date.astimezone(tz=None)

    async for data, sensor_name, measure_unit in fetch_data(start_date, end_date):
        create_spreadsheet(data, sensor_name, measure_unit)


def main(start_date: datetime, end_date: Optional[datetime]):
    asyncio.run(run_report_generation(start_date, end_date))


if __name__ == "__main__":
    typer.run(main)
