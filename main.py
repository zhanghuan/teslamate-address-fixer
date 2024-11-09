#!/usr/bin/env python3

import requests
import psycopg
import time
import argparse
import logging
from typing import Optional, Dict
from dataclasses import dataclass
from datetime import datetime
import json


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DBConfig:
    host: str
    port: str
    user: str
    password: str
    database: str

    def from_args(args:argparse.Namespace) -> 'DBConfig':
        return DBConfig(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database
        )

@dataclass
class TeslaMateAddress:
    latitude: float
    longitude: float

    display_name: Optional[str] = None
    name: Optional[str] = None
    house_number: Optional[str] = None
    road: Optional[str] = None
    neighbourhood: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    postcode: Optional[str] = None
    state: Optional[str] = None
    state_district: Optional[str] = None
    country: Optional[str] = None
    raw: Optional[str] = None
    inserted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    osm_id: Optional[int] = None
    osm_type: Optional[str] = None

    def __post_init__(self):
        assert self.latitude is not None and self.longitude is not None

    # This is how TeslaMate makes "name", as OSM returns ''
    #   defp name(%{road: road, house_number: number}) when not is_nil(road) and not is_nil(number),
    #     do: "#{road} #{number}"
    #   defp name(%{road: road}) when not is_nil(road), do: road
    #   defp name(_), do: ""
    @staticmethod
    def _get_name(road: Optional[str], house_number: Optional[str]) -> str:
        if road and house_number:
            return f"{road} {house_number}"
        elif road:
            return road
        return ""

    @classmethod
    def from_coord(cls, latitude:float, longitude:float, timeout:int = 5, proxy:Optional[str] = None) -> 'TeslaMateAddress':
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}"
        headers = {
            'User-Agent': 'TeslaMateAddressFixer/1.0'
        }

        try:
            proxies = {'http': proxy, 'https': proxy} if proxy else None
            response = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            response.raise_for_status()

            data = response.json()
            address = data.get('address', {})

            inst = cls(latitude=latitude, longitude=longitude)
            # Like TeslaMate, don't overwrite latitude and longitude with OSM data, even it has higher precision
            # self.latitude       = float(data.get('lat', self.latitude))
            # self.longitude      = float(data.get('lon', self.longitude))
            inst.display_name   = data.get('display_name', '')
            inst.name           = cls._get_name(address.get('road'), address.get('house_number'))
            inst.house_number   = address.get('house_number', '')
            inst.road           = address.get('road', '')
            inst.neighbourhood  = address.get('neighbourhood', '')
            inst.city           = address.get('city', '')
            inst.county         = address.get('county', '')
            inst.postcode       = address.get('postcode', '')
            inst.state          = address.get('state', '')
            inst.state_district = address.get('state_district', '')
            inst.country        = address.get('country', '')
            inst.raw            = json.dumps(address, ensure_ascii=False)
            inst.osm_id         = int(data.get('osm_id', 0))
            inst.osm_type       = data.get('osm_type', '')
            return inst
        except requests.RequestException as e:
            logger.error(f"Error querying OpenStreetMap: {e}")
            return None

class TeslaMateAddressFixer:
    OSM_RESOLVE_INTERVAL = 1000

    def __init__(self, db_config:DBConfig, proxy:Optional[str] = None, timeout:int = 5, dry_run:bool = False, verbose:bool = False):
        self.db_config = db_config
        self.proxy = proxy
        self.timeout = timeout
        self.dry_run = dry_run
        self.verbose = verbose
        self.conn = None

    def connect_db(self):
        try:
            self.conn = psycopg.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                dbname=self.db_config.database
            )
            self.conn.autocommit = False
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _resolve_position_id(self, position_id:int) -> Optional[TeslaMateAddress]:
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT latitude, longitude 
                FROM positions 
                WHERE id = {position_id}
            """)
            row = cursor.fetchone()
            if row:
                lat, lon = row
                address = TeslaMateAddress.from_coord(lat, lon, timeout=self.timeout, proxy=self.proxy)
                time.sleep(self.OSM_RESOLVE_INTERVAL / 1000)
                if address:
                    return address
        return None

    def fix_missing_addresses(self):
        address = None
        try:
            with self.conn.cursor() as cursor:
                # Get drives with missing addresses
                cursor.execute("""
                    SELECT id, start_address_id, start_position_id, end_address_id, end_position_id 
                    FROM drives
                    WHERE start_address_id IS NULL OR end_address_id IS NULL
                """)
                drives = cursor.fetchall()

                # Get charges with missing addresses
                cursor.execute("""
                    SELECT id, address_id, position_id
                    FROM charging_processes 
                    WHERE address_id IS NULL
                """)
                charges = cursor.fetchall()

            # Fix drives
            for drive_id, start_addr_id, start_pos_id, end_addr_id, end_pos_id in drives:
                if start_addr_id is None:
                    assert start_pos_id is not None, f"Drive #{drive_id} has no start position"
                    address = self._resolve_position_id(start_pos_id)
                    if address:
                        self._fix_drive_address(drive_id, 'start', address)
                    logger.info(f"Fix start address for drive #{drive_id}: ({address.latitude}, {address.longitude}) => {address.display_name}")
                if end_addr_id is None:
                    assert end_pos_id is not None, f"Drive #{drive_id} has no end position"
                    address = self._resolve_position_id(end_pos_id)
                    if address:
                        self._fix_drive_address(drive_id, 'end', address)
                    logger.info(f"Fix end address for drive #{drive_id}: ({address.latitude}, {address.longitude}) => {address.display_name}")

            # Fix charges
            for charge_id, addr_id, pos_id in charges:
                assert pos_id is not None, f"Charge #{charge_id} has no position"
                address = self._resolve_position_id(pos_id)
                if address:
                    self._fix_charge_address(charge_id, address)
                logger.info(f"Fix charge #{charge_id}: ({address.latitude}, {address.longitude}) => {address.display_name}")
        except Exception as e:
            logger.error(f"Error updating address {address.display_name if address else 'unknown'}: {e}")
            self.conn.rollback()

    def _fix_drive_address(self, drive_id:int, type:str, address:TeslaMateAddress):
        with self.conn.cursor() as cursor:
            address_id = self._query_or_add_address(cursor, address)
            if self.verbose:
                logger.info(f"Update drive #{drive_id} {type} address {address.display_name}")
            if not self.dry_run:
                cursor.execute(f"""
                    UPDATE drives
                    SET {type}_address_id = {address_id}
                    WHERE id = {drive_id}
                """)
                self.conn.commit()

    def _fix_charge_address(self, charge_id:int, address:TeslaMateAddress):
        with self.conn.cursor() as cursor:
            address_id = self._query_or_add_address(cursor, address)
            if self.verbose:
                logger.info(f"Update charge #{charge_id} with address {address.display_name}")
            if not self.dry_run:
                cursor.execute(f"""
                    UPDATE charging_processes
                    SET address_id = {address_id}
                    WHERE id = {charge_id}
                """)
                self.conn.commit()

    def _query_address(self, cursor, address:TeslaMateAddress) -> Optional[int]:
        cursor.execute(f"""
            SELECT id FROM addresses WHERE osm_id = {address.osm_id} AND osm_type = '{address.osm_type}'
        """)
        row = cursor.fetchone()
        return row[0] if row else None

    def _insert_new_address(self, cursor, address:TeslaMateAddress) -> Optional[int]:
        if self.dry_run:
            return None
        # Properly encode the raw JSON string
        cursor.execute(f"""
            INSERT INTO addresses (
                display_name,
                latitude,
                longitude,
                name,
                house_number,
                road,
                neighbourhood,
                city,
                county,
                postcode,
                state,
                state_district,
                country,
                raw,
                inserted_at,
                updated_at,
                osm_id,
                osm_type)
            VALUES (
                '{address.display_name}',
                {address.latitude},
                {address.longitude},
                '{address.name}',
                '{address.house_number}',
                '{address.road}',
                '{address.neighbourhood}',
                '{address.city}',
                '{address.county}',
                '{address.postcode}',
                '{address.state}',
                '{address.state_district}',
                '{address.country}',
                '{address.raw}'::jsonb,
                NOW(),
                NOW(),
                {address.osm_id},
                '{address.osm_type}')
            RETURNING id
        """)
        address_id = cursor.fetchone()[0]
        return address_id

    def _query_or_add_address(self, cursor, address:TeslaMateAddress) -> Optional[int]:
        address_id = self._query_address(cursor, address)
        if address_id:
            if self.verbose:
                logger.info(f"Use existing address {address_id} ({address.osm_id} {address.osm_type}) for '{address.display_name}'")
            return address_id

        if self.verbose:
            logger.info(f"Insert new address ({address.osm_id} {address.osm_type}): '{address.display_name}'")
        return self._insert_new_address(cursor, address)

def main(args):
    db_config = DBConfig.from_args(args)

    fixer = TeslaMateAddressFixer(db_config, args.proxy, args.timeout, args.dry_run, args.verbose)
    fixer.connect_db()

    try:
        if args.interval:
            logger.info(f"Running in daemon mode with {args.interval} minute interval")
            while True:
                fixer.fix_missing_addresses()
                time.sleep(args.interval * 60)
        else:
            fixer.fix_missing_addresses()
    finally:
        if fixer.conn:
            fixer.conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix TeslaMate missing addresses')
    parser.add_argument('-H', '--host', type=str, default='127.0.0.1', dest='host', help='Database host')
    parser.add_argument('-p', '--port', type=str, default='5432', dest='port', help='Database port')
    parser.add_argument('-u', '--user', type=str, default='teslamate', dest='user', help='Database user')
    parser.add_argument('-w', '--password', type=str, default='', dest='password', help='Database password')
    parser.add_argument('-d', '--database', type=str, default='teslamate', dest='database', help='Database name')
    parser.add_argument('-x', '--proxy', type=str, dest='proxy', help='HTTP proxy URL')
    parser.add_argument('--timeout', type=int, default=5, dest='timeout', help='OSM request timeout')
    parser.add_argument('--dry-run', default=False, action='store_true', dest='dry_run', help='Dry run mode')
    parser.add_argument('--verbose', default=False, action='store_true', dest='verbose', help='Verbose mode')
    parser.add_argument('--interval', type=int, dest='interval', help='Run interval in minutes (daemon mode)')

    args = parser.parse_args()

    main(args)
