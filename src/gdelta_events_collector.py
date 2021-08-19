import sys
from datetime import datetime
from os import path, mkdir, remove
from time import sleep
from zipfile import ZipFile

import pandas as pd
import requests


class GDeltaEventsCollector:
    MERGED_EVENTS_FILENAME = 'merged.parquet'

    def __init__(self, last_update_url: str, events_dir: str, merged_events_filename: str):
        self.merged_events_filename = merged_events_filename
        self.events_dir = events_dir
        self.last_update_url = last_update_url
        self.last_events_zip_url = None

        self.__create_dummy_merged_parquet()

    @staticmethod
    def __log(msg: str):
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '[INFO]', msg)
        sleep(0.1)  # Wait a little while afterwards, so logs can look sequential.

    def __create_dummy_merged_parquet(self):
        if not path.exists(self.events_dir):
            self.__log('Events directory: "{self.events_dir}" does NOT exist, creating..')
            mkdir(self.events_dir)
            self.__log(f'Events directory: "{self.events_dir}" has been created successfully.')
        else:
            self.__log(f'Events directory: "{self.events_dir}" already exists.')

        if not path.exists(self.merged_events_filepath):
            self.__log(f'Merged Events file: "{self.merged_events_filepath}" does NOT exist, creating..')
            pd.DataFrame().to_parquet(path=self.merged_events_filepath)
            self.__log(f'Dummy Merged Events file: "{self.merged_events_filepath}" has been created successfully.')
        else:
            self.__log(f'Merged Events file: "{self.merged_events_filepath}" already exists.')

    @property
    def merged_events_filepath(self):
        return f'{self.events_dir}/{self.merged_events_filename}'

    @staticmethod
    def __download_file(url: str, target_filepath: str) -> None:
        with open(target_filepath, 'wb') as fp:
            response = requests.get(url)
            fp.write(response.content)

    @staticmethod
    def __extract_zip(source_zipfile: str, target_dir: str) -> None:
        with ZipFile(source_zipfile) as zip_ref:
            for inner_file in zip_ref.namelist():
                zip_ref.extract(member=inner_file, path=target_dir)

    @staticmethod
    def __read_new_events(filepath: str) -> pd.DataFrame:
        df_new_events = pd.read_csv(filepath, sep='\t', header=None).drop_duplicates()
        num_cols = df_new_events.shape[1]
        zero_padding_size = len(str(num_cols))
        return df_new_events.rename(columns={i: str(i).zfill(zero_padding_size) for i in df_new_events.columns})

    def __extract_latest_events_zip_url(self) -> str:
        gdelta_latest_links_with_metadata = requests.get(self.last_update_url).text
        events_link_with_metadata = gdelta_latest_links_with_metadata.splitlines()[0]
        return events_link_with_metadata.split(' ')[-1]

    def collect_periodically(self, period_in_secs: int, keep_raw_events_file: bool = True):
        if period_in_secs <= 0:
            raise ValueError('Period should be greater than zero!')

        self.__log('Periodical collection is being started..')
        self.__log('Press Ctrl+C to exit.')
        try:
            while True:
                try:
                    self.collect(keep_raw_events_file)
                except requests.exceptions.ConnectionError:
                    print('[WARN] Connection refused due to sending more requests at a time than usual probably.')
                self.__log(f'Waiting {period_in_secs} seconds before the next collection..')
                sleep(period_in_secs)
        except KeyboardInterrupt:
            sys.exit('[INFO] Detected a keyboard interrupt, exiting..')

    def __add_new_events_to_merged_events(self, df_new_events: pd.DataFrame) -> None:
        df_merged_events = pd.read_parquet(self.merged_events_filepath)
        df_merged_events = df_merged_events.append(df_new_events).drop_duplicates().reset_index(drop=True)
        df_merged_events.to_parquet(self.merged_events_filepath)

    def collect(self, keep_raw_events_file: bool):
        events_zip_url = self.__extract_latest_events_zip_url()
        if self.last_events_zip_url == events_zip_url:
            self.__log(f'No new GDelta Events Zip URL, same with the last one: "{self.last_events_zip_url}".')
            return
        self.last_events_zip_url = events_zip_url
        self.__log(f'Detected new GDelta Events Zip URL: "{events_zip_url}".')

        events_zip_filename = events_zip_url.split('/')[-1]
        events_zip_filepath = f'{self.events_dir}/{events_zip_filename}'
        events_filepath = events_zip_filepath.replace('.zip', '')

        self.__download_file(url=events_zip_url, target_filepath=f'{self.events_dir}/{events_zip_filename}')
        self.__extract_zip(source_zipfile=events_zip_filepath, target_dir=self.events_dir)

        df_new_events = self.__read_new_events(filepath=events_filepath)
        self.__add_new_events_to_merged_events(df_new_events=df_new_events)

        self.__log(f'Collected and merged new GDelta Events Zip URL: "{events_zip_url}" successfully.')

        if not keep_raw_events_file:
            for file in [events_zip_filepath, events_filepath]:
                remove(file)
            self.__log(f'Deleted the remaining raw event files: ["{events_zip_filepath}", "{events_filepath}"]')
