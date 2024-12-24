#!/usr/bin/env python
import asyncio
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from enum import Enum
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


class Granularity(str, Enum):
    daily = 'daily'
    monthly = 'monthly'
    yearly = 'yearly'

logger = logging.getLogger()




async def fetch_json(session: aiohttp.ClientSession, url: str, retry_times: Optional[int] = 3):
    retries_left = retry_times

    while retries_left > 0:
        try:
            async with session.get(url, headers=AUTH_HEADERS) as response:
                response.raise_for_status()  # Raises an exception for 4xx/5xx responses
                return await response.json()

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retries_left -= 1
            if retries_left > 0:
                wait_time = (retry_times - retries_left) * 2  # Increasing wait time on each retry
                print(f"Error fetching data from {url}. Retrying in {wait_time} seconds... ({retries_left} retries left)")
                await asyncio.sleep(wait_time)
            else:
                print(f"Failed to fetch data from {url} after {retry_times} attempts.")
                raise e  # Re-raise the exception after all retries are exhausted


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



def create_spreadsheet(data, sensor_name, measure_unit, granularity='daily', reports_folder_path=Path('reports')):
    logger.info(f'Creating {granularity} excel report for:\t{sensor_name}')

    # Flatten the data from each device into a single list
    data = [entry_data for device_data in data for entry_data in device_data]
    df = pandas.DataFrame.from_dict(data).dropna()

    # Convert timestamps to datetime
    df['datetime'] = df['time'].apply(lambda x: unix_timestamp_to_datetime(x))

    # Generate intervals based on granularity
    df['interval'] = generate_intervals(df, granularity)

    # Calculate the mean based on the granularity
    average_per_interval, average_per_interval_per_device = calculate_mean(df, sensor_name)

    # Create reports folder if it doesn't exist
    if not reports_folder_path.exists():
        reports_folder_path.mkdir(parents=True)
    elif not reports_folder_path.is_dir():
        raise Exception(f"Location for reports '{reports_folder_path}' is a file! Need a folder")

    # Determine the interval range for the report name
    start_interval = df['interval'].min()
    end_interval = df['interval'].max()
    interval_str = f'{start_interval}-{end_interval}'

    # Construct the report file name
    report_path = reports_folder_path / f'{sensor_name}_{granularity}_report_{interval_str}.xlsx'

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pandas.ExcelWriter(report_path, engine='xlsxwriter')

    # Write the overall average to Excel
    average_per_interval.to_excel(writer, sheet_name='all')
    chart = create_chart(average_per_interval, writer.book, 'all', sensor_name, measure_unit)
    writer.sheets['all'].insert_chart('D5', chart, {'x_scale': 2, 'y_scale': 1})

    # Write the average per device to Excel
    for group_name, df_group in average_per_interval_per_device.groupby('device_id'):
        df_group[sensor_name].to_excel(writer, sheet_name=group_name)
        chart = create_chart(df_group, writer.book, group_name, sensor_name, measure_unit)
        writer.sheets[group_name].insert_chart('D5', chart, {'x_scale': 2, 'y_scale': 1})

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

def generate_intervals(df, granularity):
    """
    Generate intervals based on the specified granularity (daily, monthly, yearly).
    """
    if granularity == 'daily':
        return df['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
    elif granularity == 'monthly':
        return df['datetime'].apply(lambda x: x.strftime('%Y-%m'))
    elif granularity == 'yearly':
        return df['datetime'].apply(lambda x: x.strftime('%Y'))
    else:
        raise ValueError(f"Invalid granularity '{granularity}' provided. Use 'daily', 'monthly', or 'yearly'.")

def calculate_mean(df, sensor_name):
    """
    Calculate the mean of the sensor data based on the provided intervals.
    Returns two DataFrames: overall average and per-device average.
    """
    # Calculate the overall average for each interval
    average_per_interval = df.groupby('interval')[sensor_name].mean().reset_index()
    average_per_interval = average_per_interval.set_index('interval')

    # Calculate the average per device for each interval
    average_per_interval_per_device = df.groupby(['device_id', 'interval'])[sensor_name].mean().reset_index()
    average_per_interval_per_device = average_per_interval_per_device.set_index('interval')

    return average_per_interval, average_per_interval_per_device


async def run_report_generation(start_date, end_date=None, granularity: Granularity = Granularity.daily):
    start_date = start_date.astimezone(tz=None)
    if end_date is not None:
        end_date = end_date.astimezone(tz=None)

    async for data, sensor_name, measure_unit in fetch_data(start_date, end_date):
        create_spreadsheet(data, sensor_name, measure_unit, granularity.value)


def main(start_date: datetime, end_date: Optional[datetime], granularity: Granularity = Granularity.daily):
    asyncio.run(run_report_generation(start_date, end_date, granularity))


if __name__ == "__main__":
    typer.run(main)
