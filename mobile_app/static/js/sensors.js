/**
 * ZeroStream - Phone Sensor Display
 * Updates all sensor visualisations inside the phone modal.
 */

const PhoneSensors = (() => {

  // ── Heading → cardinal direction ───────────────────────────────────────
  function _headingToCardinal(deg) {
    const dirs = ['N','NE','E','SE','S','SW','W','NW'];
    return dirs[Math.round(deg / 45) % 8];
  }

  // ── Update all sensor displays ─────────────────────────────────────────
  function update(payload) {
    if (!payload) return;

    // Device name & status bar
    _setText('modalDeviceName', payload.device_name || payload.connection_id);
    _setText('modalBattery',    `🔋 ${payload.battery_pct ?? 100}%`);

    // ── Pitch bar ──────────────────────────────────────────────────────
    const pitch    = payload.pitch_deg ?? 0;
    const pitchPct = ((pitch + 90) / 180) * 100;   // map -90..+90 → 0..100%
    const pitchBar = document.getElementById('modalPitchBar');
    if (pitchBar) {
      const offset = pitch >= 0
        ? `left:50%; width:${Math.min(pitchPct - 50, 50)}%`
        : `left:${pitchPct}%; width:${50 - pitchPct}%`;
      pitchBar.style.cssText = offset;
      pitchBar.style.background = pitch >= 0
        ? 'linear-gradient(90deg,#3b82f6,#06b6d4)'
        : 'linear-gradient(90deg,#f59e0b,#ef4444)';
    }
    _setText('modalPitch', `${pitch.toFixed(1)}°`);

    // ── Compass ────────────────────────────────────────────────────────
    const heading  = payload.heading_deg ?? 0;
    const compass  = document.getElementById('modalCompass');
    const needle   = compass?.querySelector('.compass-needle');
    if (needle) {
      needle.style.transform =
        `translateX(-50%) translateY(-100%) rotate(${heading}deg)`;
    }
    _setText('modalHeading', `${heading.toFixed(0)}° ${_headingToCardinal(heading)}`);

    // ── Acceleration ───────────────────────────────────────────────────
    _setText('modalAccelX', (payload.accel_x ?? 0).toFixed(2));
    _setText('modalAccelY', (payload.accel_y ?? 0).toFixed(2));
    _setText('modalAccelZ', (payload.accel_z ?? 0).toFixed(2));

    const mag    = payload.accel_magnitude ?? 0;
    const magPct = Math.min((mag / 20) * 100, 100);
    const magBar = document.getElementById('modalAccelBar');
    if (magBar) magBar.style.width = `${magPct}%`;

    // ── Gyroscope ──────────────────────────────────────────────────────
    _setText('modalGyroX', (payload.gyro_x ?? 0).toFixed(2));
    _setText('modalGyroY', (payload.gyro_y ?? 0).toFixed(2));
    _setText('modalGyroZ', (payload.gyro_z ?? 0).toFixed(2));

    // ── Location ───────────────────────────────────────────────────────
    _setText('modalLat', (payload.latitude  ?? 0).toFixed(6));
    _setText('modalLon', (payload.longitude ?? 0).toFixed(6));

    // ── Speed & Roll ───────────────────────────────────────────────────
    _setText('modalSpeed', `${(payload.speed_kmh ?? 0).toFixed(1)} km/h`);
    _setText('modalRoll',  `${(payload.roll_deg  ?? 0).toFixed(1)}°`);

    // ── Update phone map marker ────────────────────────────────────────
    if (payload.latitude && payload.longitude) {
      PhoneMap.updateMarker(payload.latitude, payload.longitude);
    }
  }

  function _setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  return { update };
})();