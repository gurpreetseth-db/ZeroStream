"""
Realistic sensor data generator.
Simulates phone motion sensors with smooth, correlated movements
rather than pure random noise - makes the demo feel authentic.
"""
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


# ── Named locations seeded per connection for realism ─────────────────────────
SEED_LOCATIONS: List[Tuple[float, float, str]] = [
    (-33.8688,  151.2093, "Sydney"),
    ( 51.5074,   -0.1278, "London"),
    ( 40.7128,  -74.0060, "New York"),
    ( 35.6762,  139.6503, "Tokyo"),
    ( 48.8566,    2.3522, "Paris"),
    (-23.5505,  -46.6333, "São Paulo"),
    ( 37.7749, -122.4194, "San Francisco"),
    (  1.3521,  103.8198, "Singapore"),
    ( 55.7558,   37.6173, "Moscow"),
    ( 28.6139,   77.2090, "New Delhi"),
    (-34.6037,  -58.3816, "Buenos Aires"),
    ( 31.2304,  121.4737, "Shanghai"),
    ( 19.0760,   72.8777, "Mumbai"),
    ( 52.5200,   13.4050, "Berlin"),
    ( 25.2048,   55.2708, "Dubai"),
    (-36.8485,  174.7633, "Auckland"),
    ( 43.6532,  -79.3832, "Toronto"),
    ( 59.9139,   10.7522, "Oslo"),
    ( 41.9028,   12.4964, "Rome"),
    (-26.2041,   28.0473, "Johannesburg"),
]

# Adjective + noun device name generator
_ADJ  = ["zeta","stream","spark","drift","pixel","wave","sync","flux","neo","arc"]
_NOUN = ["wave","shine","mesh","flow","pulse","link","node","core","beam","grid"]


def _make_device_name(connection_id: str) -> str:
    """Deterministic device name from connection_id."""
    h = abs(hash(connection_id))
    return f"{_ADJ[h % len(_ADJ)]}{_NOUN[(h // 10) % len(_NOUN)]}{h % 1000}"


@dataclass
class ConnectionState:
    """
    Tracks the evolving state of one simulated device.
    Uses smooth random-walk physics so sensor values look real.
    """
    connection_id: str
    device_name:   str
    seed_lat:      float
    seed_lon:      float
    city:          str

    # Physics state (updated each tick)
    lat:           float = 0.0
    lon:           float = 0.0
    altitude_m:    float = 0.0
    heading_deg:   float = 0.0
    pitch_deg:     float = 0.0
    roll_deg:      float = 0.0
    accel_x:       float = 0.0
    accel_y:       float = 0.0
    accel_z:       float = 9.81   # gravity at rest
    gyro_x:        float = 0.0
    gyro_y:        float = 0.0
    gyro_z:        float = 0.0
    speed_kmh:     float = 0.0
    battery_pct:   int   = 100
    signal_strength: int = -65

    # Internal drift accumulators
    _heading_vel:  float = field(default=0.0, repr=False)
    _pitch_vel:    float = field(default=0.0, repr=False)
    _roll_vel:     float = field(default=0.0, repr=False)
    _speed_vel:    float = field(default=0.0, repr=False)
    _event_count:  int   = field(default=0,   repr=False)
    _created_at:   float = field(default_factory=time.time, repr=False)

    def __post_init__(self):
        self.lat = self.seed_lat + random.uniform(-0.01, 0.01)
        self.lon = self.seed_lon + random.uniform(-0.01, 0.01)
        self.altitude_m  = random.uniform(0, 200)
        self.heading_deg = random.uniform(0, 360)
        self.pitch_deg   = random.uniform(-15, 15)
        self.roll_deg    = random.uniform(-10, 10)
        self.speed_kmh   = random.uniform(0, 60)
        self.battery_pct = random.randint(20, 100)
        self._heading_vel = random.uniform(-2, 2)
        self._pitch_vel   = random.uniform(-1, 1)
        self._roll_vel    = random.uniform(-1, 1)
        self._speed_vel   = random.uniform(-5, 5)

    def _smooth_walk(self, value: float, velocity: float,
                     min_v: float, max_v: float,
                     accel_range: float = 0.5,
                     damping: float = 0.95) -> Tuple[float, float]:
        """Smooth random walk with damping - feels like real sensor data."""
        velocity = velocity * damping + random.uniform(-accel_range, accel_range)
        velocity = max(-accel_range * 4, min(accel_range * 4, velocity))
        value = value + velocity
        # Bounce at boundaries
        if value < min_v:
            value = min_v
            velocity = abs(velocity) * 0.5
        elif value > max_v:
            value = max_v
            velocity = -abs(velocity) * 0.5
        return value, velocity

    def tick(self) -> dict:
        """Advance physics by one time step and return sensor payload."""
        self._event_count += 1

        # ── Heading: wraps 0-360 ──────────────────────────────────────────────
        self._heading_vel = (
            self._heading_vel * 0.95
            + random.uniform(-1.5, 1.5)
        )
        self.heading_deg = (self.heading_deg + self._heading_vel) % 360

        # ── Pitch & Roll ──────────────────────────────────────────────────────
        self.pitch_deg, self._pitch_vel = self._smooth_walk(
            self.pitch_deg, self._pitch_vel, -45, 45, 0.8
        )
        self.roll_deg, self._roll_vel = self._smooth_walk(
            self.roll_deg, self._roll_vel, -90, 90, 1.0
        )

        # ── Speed ─────────────────────────────────────────────────────────────
        self.speed_kmh, self._speed_vel = self._smooth_walk(
            self.speed_kmh, self._speed_vel, 0, 120, 3.0
        )

        # ── Acceleration (gravity + motion) ───────────────────────────────────
        pitch_r = math.radians(self.pitch_deg)
        roll_r  = math.radians(self.roll_deg)
        g = 9.81
        self.accel_x = g * math.sin(pitch_r) + random.gauss(0, 0.15)
        self.accel_y = g * math.sin(roll_r)  + random.gauss(0, 0.15)
        self.accel_z = g * math.cos(pitch_r) * math.cos(roll_r) + random.gauss(0, 0.1)
        self.accel_magnitude = math.sqrt(
            self.accel_x**2 + self.accel_y**2 + self.accel_z**2
        )

        # ── Gyroscope ─────────────────────────────────────────────────────────
        self.gyro_x = self._pitch_vel * 10 + random.gauss(0, 0.5)
        self.gyro_y = self._roll_vel  * 10 + random.gauss(0, 0.5)
        self.gyro_z = self._heading_vel * 5 + random.gauss(0, 0.3)

        # ── GPS movement (realistic walking/driving pace) ─────────────────────
        # Speed in meters/second, convert heading to direction
        speed_ms  = self.speed_kmh / 3.6
        heading_r = math.radians(self.heading_deg)
        
        # Movement per tick (assuming ~1 second intervals)
        # At 5 km/h walking pace, should move ~1.4m per second
        # At 50 km/h driving, should move ~14m per second
        # Scale factor makes movement visible on map
        movement_scale = 1.0  # Full movement per tick
        
        # Latitude: 1 degree ≈ 111,320 meters
        delta_lat = (speed_ms * math.cos(heading_r) * movement_scale) / 111_320
        
        # Longitude: varies by latitude, 1 degree ≈ 111,320 * cos(lat) meters  
        delta_lon = (speed_ms * math.sin(heading_r) * movement_scale) / (
            111_320 * math.cos(math.radians(self.lat)) + 1e-10
        )
        
        # Apply movement with small GPS noise
        self.lat = max(-85, min(85,  self.lat + delta_lat + random.gauss(0, 0.00002)))
        self.lon = max(-180, min(180, self.lon + delta_lon + random.gauss(0, 0.00002)))
        self.altitude_m = max(0, self.altitude_m + random.gauss(0, 0.5))

        # ── Battery drain ─────────────────────────────────────────────────────
        if self._event_count % 300 == 0 and self.battery_pct > 1:
            self.battery_pct -= 1

        # ── Signal fluctuation ────────────────────────────────────────────────
        self.signal_strength = max(
            -100, min(-40, self.signal_strength + random.randint(-2, 2))
        )

        payload_bytes = 256 + random.randint(-20, 40)

        return {
            "event_id":         str(uuid.uuid4()),
            "connection_id":    self.connection_id,
            "device_name":      self.device_name,
            "event_timestamp":  datetime.now(timezone.utc).isoformat(),
            "event_date":       datetime.now(timezone.utc).date().isoformat(),
            "ingested_at":      datetime.now(timezone.utc).isoformat(),
            "latitude":         round(self.lat, 6),
            "longitude":        round(self.lon, 6),
            "altitude_m":       round(self.altitude_m, 2),
            "heading_deg":      round(self.heading_deg, 2),
            "pitch_deg":        round(self.pitch_deg, 2),
            "roll_deg":         round(self.roll_deg, 2),
            "accel_x":          round(self.accel_x, 4),
            "accel_y":          round(self.accel_y, 4),
            "accel_z":          round(self.accel_z, 4),
            "accel_magnitude":  round(self.accel_magnitude, 4),
            "gyro_x":           round(self.gyro_x, 4),
            "gyro_y":           round(self.gyro_y, 4),
            "gyro_z":           round(self.gyro_z, 4),
            "speed_kmh":        round(self.speed_kmh, 2),
            "battery_pct":      self.battery_pct,
            "signal_strength":  self.signal_strength,
            "payload_bytes":    payload_bytes,
        }


class DataGeneratorPool:
    """Manages a pool of ConnectionState objects."""

    def __init__(self):
        self._connections: Dict[str, ConnectionState] = {}

    def set_connection_count(self, count: int) -> List[str]:
        """
        Grow or shrink the pool to exactly `count` connections.
        Returns list of all active connection IDs.
        """
        current_ids = list(self._connections.keys())
        current_count = len(current_ids)

        if count > current_count:
            for _ in range(count - current_count):
                cid = str(uuid.uuid4())[:8]
                loc = random.choice(SEED_LOCATIONS)
                self._connections[cid] = ConnectionState(
                    connection_id=cid,
                    device_name=_make_device_name(cid),
                    seed_lat=loc[0],
                    seed_lon=loc[1],
                    city=loc[2],
                )
        elif count < current_count:
            to_remove = current_ids[count:]
            for cid in to_remove:
                del self._connections[cid]

        return list(self._connections.keys())

    def tick_all(self) -> List[dict]:
        """Advance all connections and return list of payloads."""
        return [state.tick() for state in self._connections.values()]

    def get_connection(self, connection_id: str) -> Optional[ConnectionState]:
        return self._connections.get(connection_id)

    def get_all_states(self) -> Dict[str, ConnectionState]:
        return dict(self._connections)

    @property
    def count(self) -> int:
        return len(self._connections)


# Global pool instance
generator_pool = DataGeneratorPool()