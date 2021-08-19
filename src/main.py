from typing import Dict

from gdelta_events_collector import GDeltaEventsCollector


def main(props: Dict):
    gec = GDeltaEventsCollector(
        last_update_url=props['GDELTA_LAST_UPDATE_URL'],
        events_dir=props['GDELTA_EVENTS_DIR'],
        merged_events_filename=props['GDELTA_MERGED_EVENTS_FILENAME']
    )
    gec.collect_periodically(props['GDELTA_EVENTS_COLLECTION_PERIOD'], props['KEEP_RAW_EVENTS_FILE'])


if __name__ == '__main__':
    PROPERTIES = {
        'GDELTA_LAST_UPDATE_URL': 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt',
        'GDELTA_EVENTS_DIR': 'data/gdelta_events',
        'GDELTA_MERGED_EVENTS_FILENAME': 'merged.parquet',
        'GDELTA_EVENTS_COLLECTION_PERIOD': 10,
        'KEEP_RAW_EVENTS_FILE': False
    }

    main(props=PROPERTIES)
