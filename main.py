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
from math import radians, cos, sin, sqrt, atan2


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

    def connect(self) -> psycopg.Connection:
        try:
            conn = psycopg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.database
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

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

    @classmethod
    def from_address(cls, **address) -> 'TeslaMateAddress':
        try:
            inst = cls(latitude=address['latitude'], longitude=address['longitude'])
            inst.display_name   = address['display_name']
            inst.name           = address['name']
            inst.house_number   = address['house_number']
            inst.road           = address['road']
            inst.neighbourhood  = address['neighbourhood']
            inst.city           = address['city']
            inst.county         = address['county']
            inst.postcode       = address['postcode']
            inst.state          = address['state']
            inst.state_district = address['state_district']
            inst.country        = address['country']
            inst.raw            = address['raw']
            inst.osm_id         = address['osm_id']
            inst.osm_type       = address['osm_type']
            return inst
        except KeyError as e:
            logger.error(f"Error creating TeslaMateAddress from address: {e}")
            return None

class TeslaMateAddressFixer:
    OSM_RESOLVE_INTERVAL = 1000

    def __init__(self, db_conn:psycopg.Connection, proxy:Optional[str] = None, timeout:int = 5, dry_run:bool = False, verbose:bool = False):
        self.db_conn = db_conn
        self.proxy = proxy
        self.timeout = timeout
        self.dry_run = dry_run
        self.verbose = verbose

    def _resolve_position_id(self, position_id:int) -> Optional[TeslaMateAddress]:
        with self.db_conn.cursor() as cursor:
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

    def execute(self):
        address = None
        try:
            with self.db_conn.cursor() as cursor:
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
            self.db_conn.conn.rollback()

    def _fix_drive_address(self, drive_id:int, type:str, address:TeslaMateAddress):
        with self.db_conn.cursor() as cursor:
            address_id = self._query_or_add_address(cursor, address)
            if self.verbose:
                logger.info(f"Update drive #{drive_id} {type} address {address.display_name}")
            if not self.dry_run:
                cursor.execute(f"""
                    UPDATE drives
                    SET {type}_address_id = {address_id}
                    WHERE id = {drive_id}
                """)
                self.db_conn.conn.commit()

    def _fix_charge_address(self, charge_id:int, address:TeslaMateAddress):
        with self.db_conn.cursor() as cursor:
            address_id = self._query_or_add_address(cursor, address)
            if self.verbose:
                logger.info(f"Update charge #{charge_id} with address {address.display_name}")
            if not self.dry_run:
                cursor.execute(f"""
                    UPDATE charging_processes
                    SET address_id = {address_id}
                    WHERE id = {charge_id}
                """)
                self.db_conn.conn.commit()

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

class TeslaMateFindNearbyAddresses:
    def __init__(self, db_conn:psycopg.Connection, radius:int = 50):
        self.db_conn = db_conn
        self.radius = radius

    def execute(self):
        def _calculate_distance(address1, address2):
            # Convert latitude and longitude from degrees to radians
            lat1, lon1 = radians(address1.latitude), radians(address1.longitude)
            lat2, lon2 = radians(address2.latitude), radians(address2.longitude)

            # Haversine formula to calculate the distance
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            radius_of_earth_m = 6371000  # in meters
            distance = radius_of_earth_m * c

            return distance

        addresses = []
        # get all addresses
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                select
                    latitude,
                    longitude,
                    display_name,
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
                    osm_id,
                    osm_type
                from addresses
            """)
            for row in cursor:
                address = TeslaMateAddress.from_address(
                    latitude=row[0],
                    longitude=row[1],
                    display_name=row[2],
                    name=row[3],
                    house_number=row[4],
                    road=row[5],
                    neighbourhood=row[6],
                    city=row[7],
                    county=row[8],
                    postcode=row[9],
                    state=row[10],
                    state_district=row[11],
                    country=row[12],
                    raw=row[13],
                    osm_id=row[14],
                    osm_type=row[15]
                )
                addresses.append(address)

        # find nearby addresses
        nearby_addresses = {}
        for i, address in enumerate(addresses):
            nearby_addresses[i] = []
            for j, other_address in enumerate(addresses[i+1:]):
                distance_m = _calculate_distance(address, other_address)
                if distance_m <= self.radius:
                    nearby_addresses[i].append((i + 1 + j, distance_m))

        nearby_addresses_str = ''
        for address_id, nearby_address_ids in nearby_addresses.items():
            address = addresses[address_id]
            if not nearby_address_ids:
                continue

            nearby_addresses_str += f"({address.latitude}, {address.longitude}): {address.display_name}\n"
            for nearby_address_id, distance_m in nearby_address_ids:
                nearby_address = addresses[nearby_address_id]
                nearby_addresses_str += f"{distance_m:>8.2f}m  ({nearby_address.latitude}, {nearby_address.longitude}): {nearby_address.display_name}\n"
            nearby_addresses_str += '\n'
        logger.info(f"Found nearby addresses:\n{nearby_addresses_str}")

def main(args):
    db_config = DBConfig.from_args(args)
    db_conn = db_config.connect()

    if args.cmd == 'fix':
        fixer = TeslaMateAddressFixer(db_conn, args.proxy, args.timeout, args.dry_run, args.verbose)
        if args.interval:
            logger.info(f"Running in daemon mode with {args.interval} minute interval")
            while True:
                fixer.execute()
                time.sleep(args.interval * 60)
        else:
            fixer.execute()
    elif args.cmd == 'find-nearby-addresses':
        finder = TeslaMateFindNearbyAddresses(db_conn, args.radius)
        finder.execute()
    else:
        raise ValueError(f"Unknown command: {args.cmd}")

    db_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TeslaMate address fixer')
    db_args = argparse.ArgumentParser(add_help=False)
    db_args.add_argument('-H', '--host', type=str, default='127.0.0.1', dest='host', help='Database host')
    db_args.add_argument('-p', '--port', type=str, default='5432', dest='port', help='Database port')
    db_args.add_argument('-u', '--user', type=str, default='teslamate', dest='user', help='Database user')
    db_args.add_argument('-w', '--password', type=str, default='', dest='password', help='Database password')
    db_args.add_argument('-d', '--database', type=str, default='teslamate', dest='database', help='Database name')
    subparsers = parser.add_subparsers(dest='cmd')
    parser_fix = subparsers.add_parser('fix', parents=[db_args,], help='Fix missing addresses')
    parser_fix.add_argument('-x', '--proxy', type=str, dest='proxy', help='HTTP proxy URL')
    parser_fix.add_argument('--timeout', type=int, default=5, dest='timeout', help='OSM request timeout')
    parser_fix.add_argument('--dry-run', default=False, action='store_true', dest='dry_run', help='Dry run mode')
    parser_fix.add_argument('--verbose', default=False, action='store_true', dest='verbose', help='Verbose mode')
    parser_fix.add_argument('--interval', type=int, dest='interval', help='Run interval in minutes (daemon mode)')
    parser_find_nearby_addresses = subparsers.add_parser('find-nearby-addresses', parents=[db_args,], help='Find nearby addresses')
    parser_find_nearby_addresses.add_argument('-r', '--radius', type=int, default=50, dest='radius', help='Find nearby address radius in meters')

    args = parser.parse_args()

    main(args)
