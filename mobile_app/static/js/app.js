/* ZeroStream Mobile App - Two-Panel Layout */

const App = (() => {
  // State
  let deviceMap = null;
  let deviceMarker = null;
  let isStreaming = false;
  let publishedCount = 0;
  let selectedDeviceId = null;
  let ws = null;
  const connections = new Map(); // connectionId -> { data, deviceName, status }

  // DOM Elements
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // ══════════════════════════════════════════════════════════════════════════
  // INITIALIZATION
  // ══════════════════════════════════════════════════════════════════════════
  async function init() {
    // Reset app state on load/refresh
    await resetAppState();
    
    // Set up slider
    const slider = $('#connectionSlider');
    const sliderValue = $('#sliderValue');
    if (slider) {
      slider.value = 0; // Reset slider to 0
      sliderValue.textContent = '0';
      slider.addEventListener('input', () => {
        sliderValue.textContent = slider.value;
      });
    }

    // Initialize map (but don't show it yet)
    initMap();

    // Update UI to show reset state
    updateDeviceCount();
    updateDeviceList();  // Clear device list UI
    updateStreamingUI();
    showEmptyState();    // Reset detail panel

    console.log('✅ ZeroStream App initialized (state reset)');
  }

  async function resetAppState() {
    try {
      const resp = await fetch('/api/reset', { method: 'POST' });
      const data = await resp.json();
      
      // Clear local state
      connections.clear();
      isStreaming = false;
      publishedCount = 0;
      selectedDeviceId = null;
      
      console.log('🔄 App state reset on load');
      return data;
    } catch (err) {
      console.error('Failed to reset app state:', err);
    }
  }

  function initMap() {
    const mapEl = $('#deviceMap');
    if (!mapEl) return;

    deviceMap = L.map('deviceMap', {
      center: [37.7749, -122.4194],
      zoom: 12,
      zoomControl: false
    });

    // Use CartoDB Voyager (lighter dark theme with visible features)
    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; CartoDB',
      maxZoom: 19
    }).addTo(deviceMap);

    L.control.zoom({ position: 'bottomright' }).addTo(deviceMap);

    // Fix map sizing issue
    setTimeout(() => deviceMap && deviceMap.invalidateSize(), 100);
  }

  // ══════════════════════════════════════════════════════════════════════════
  // CONNECTION MANAGEMENT via REST API
  // ══════════════════════════════════════════════════════════════════════════
  async function loadConnections() {
    try {
      const resp = await fetch('/api/connections');
      const data = await resp.json();

      connections.clear();
      data.connections.forEach((conn) => {
        connections.set(conn.connection_id, {
          deviceName: conn.device_name,
          city: conn.city,
          data: conn.latest || {},
          status: conn.active ? 'streaming' : 'idle',
          battery: conn.battery_pct,
          signal: conn.signal_strength
        });
      });

      isStreaming = data.streaming;
      updateStreamingUI();
      updateDeviceList();
      updateDeviceCount();

      console.log(`📱 Loaded ${connections.size} connections`);
    } catch (err) {
      console.error('Failed to load connections:', err);
    }
  }

  async function applyConnectionCount() {
    const slider = $('#connectionSlider');
    const count = parseInt(slider.value, 10);

    try {
      // Configure via REST API
      const resp = await fetch('/api/stream/configure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection_count: count,
          active: false
        })
      });

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      // Reload connections
      await loadConnections();

      // Clear selection
      selectedDeviceId = null;
      showEmptyState();

      console.log(`📱 Applied ${count} device connections`);
    } catch (err) {
      console.error('Failed to apply connections:', err);
    }
  }

  function updateDeviceList() {
    const listEl = $('#deviceList');
    if (!listEl) return;

    if (connections.size === 0) {
      listEl.innerHTML = `
        <div class="device-list-empty">
          <div class="empty-icon">📡</div>
          <div>Click "Apply" to create devices</div>
        </div>
      `;
      return;
    }

    listEl.innerHTML = '';
    connections.forEach((conn, id) => {
      const card = document.createElement('div');
      card.className = `device-card ${id === selectedDeviceId ? 'selected' : ''}`;
      card.dataset.id = id;
      card.onclick = () => selectDevice(id);

      const isActive = conn.status === 'streaming';
      card.innerHTML = `
        <div class="device-card-header">
          <span class="device-card-name">${conn.deviceName}</span>
          <span class="device-card-status ${isActive ? 'active' : ''}"></span>
        </div>
        <div class="device-card-id">${id.substring(0, 20)}...</div>
      `;
      listEl.appendChild(card);
    });
  }

  function updateDeviceCount() {
    const countEl = $('#deviceCount');
    if (countEl) countEl.textContent = connections.size;
  }

  // ══════════════════════════════════════════════════════════════════════════
  // DEVICE SELECTION
  // ══════════════════════════════════════════════════════════════════════════
  function selectDevice(id) {
    selectedDeviceId = id;
    const conn = connections.get(id);
    if (!conn) return;

    // Update selection in list
    $$('.device-card').forEach((card) => {
      card.classList.toggle('selected', card.dataset.id === id);
    });

    // Show detail content
    $('#detailEmpty').style.display = 'none';
    $('#detailContent').style.display = 'flex';

    // Update header
    $('#selectedDeviceName').textContent = conn.deviceName;
    $('#selectedDeviceId').textContent = id.substring(0, 25) + '...';
    $('#selectedDeviceStatus').textContent = conn.status === 'streaming' ? '● Streaming' : '○ Idle';
    $('#selectedDeviceStatus').className = `device-status ${conn.status === 'streaming' ? 'active' : 'inactive'}`;

    // Update detail view
    updateDeviceDetail(conn.data);

    // Center map on device - use latitude/longitude from data_generator.py
    const lat = conn.data.latitude || 37.7749;
    const lon = conn.data.longitude || -122.4194;

    if (deviceMap) {
      deviceMap.setView([lat, lon], 14);

      if (deviceMarker) {
        deviceMarker.setLatLng([lat, lon]);
      } else {
        deviceMarker = L.circleMarker([lat, lon], {
          radius: 10,
          fillColor: '#1DB954',
          fillOpacity: 1,
          color: '#fff',
          weight: 2
        }).addTo(deviceMap);
      }

      // Fix map rendering
      setTimeout(() => deviceMap.invalidateSize(), 50);
    }
  }

  function showEmptyState() {
    $('#detailEmpty').style.display = 'flex';
    $('#detailContent').style.display = 'none';
  }

  function updateDeviceDetail(data) {
    if (!data) data = {};

    // Battery, signal, speed - match data_generator.py field names
    const battery = data.battery_pct ?? 100;
    const signal = data.signal_strength ?? -50;
    const speed = data.speed_kmh ?? 0;

    $('#selectedBattery').textContent = `${Math.round(battery)}%`;
    $('#selectedSignal').textContent = `${Math.round(signal)} dBm`;
    $('#selectedSpeed').textContent = `${speed.toFixed(1)} km/h`;

    // Pitch gauge - field is pitch_deg
    const pitch = data.pitch_deg ?? 0;
    const pitchPct = Math.min(Math.abs(pitch) / 90, 1) * 50;
    const pitchBar = $('#pitchBar');
    if (pitchBar) {
      if (pitch >= 0) {
        pitchBar.style.left = '50%';
        pitchBar.style.width = `${pitchPct}%`;
      } else {
        pitchBar.style.left = `${50 - pitchPct}%`;
        pitchBar.style.width = `${pitchPct}%`;
      }
    }
    $('#pitchValue').textContent = `${pitch >= 0 ? '+' : ''}${pitch.toFixed(1)}°`;

    // Roll gauge - field is roll_deg
    const roll = data.roll_deg ?? 0;
    const rollPct = Math.min(Math.abs(roll) / 90, 1) * 50;
    const rollBar = $('#rollBar');
    if (rollBar) {
      if (roll >= 0) {
        rollBar.style.left = '50%';
        rollBar.style.width = `${rollPct}%`;
      } else {
        rollBar.style.left = `${50 - rollPct}%`;
        rollBar.style.width = `${rollPct}%`;
      }
    }
    $('#rollValue').textContent = `${roll >= 0 ? '+' : ''}${roll.toFixed(1)}°`;

    // Compass heading - field is heading_deg
    const heading = data.heading_deg ?? 0;
    const pointer = $('#compassPointer');
    if (pointer) {
      pointer.style.transform = `translate(-50%, -100%) rotate(${heading}deg)`;
    }
    $('#headingValue').textContent = `${Math.round(heading)}°`;

    // Acceleration - fields are accel_x, accel_y, accel_z
    const accelX = data.accel_x ?? 0;
    const accelY = data.accel_y ?? 0;
    const accelZ = data.accel_z ?? 9.8;
    $('#accelX').textContent = accelX.toFixed(2);
    $('#accelY').textContent = accelY.toFixed(2);
    $('#accelZ').textContent = accelZ.toFixed(2);

    // Gyroscope - fields are gyro_x, gyro_y, gyro_z
    const gyroX = data.gyro_x ?? 0;
    const gyroY = data.gyro_y ?? 0;
    const gyroZ = data.gyro_z ?? 0;
    $('#gyroAlpha').textContent = gyroX.toFixed(2);
    $('#gyroBeta').textContent = gyroY.toFixed(2);
    $('#gyroGamma').textContent = gyroZ.toFixed(2);

    // Location
    const lat = data.latitude ?? 0;
    const lon = data.longitude ?? 0;
    const alt = data.altitude_m ?? 0;
    $('#locationCoords').textContent = `Lat ${lat.toFixed(5)}, Lon ${lon.toFixed(5)}`;
    $('#locationAlt').textContent = `ALT ${Math.round(alt)}m`;

    // Update map marker
    if (deviceMap && deviceMarker && lat && lon) {
      deviceMarker.setLatLng([lat, lon]);
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // STREAMING via REST API + WebSocket
  // ══════════════════════════════════════════════════════════════════════════
  async function toggleStreaming() {
    if (isStreaming) {
      await stopStreaming();
    } else {
      await startStreaming();
    }
  }

  async function startStreaming() {
    if (connections.size === 0) {
      console.warn('No devices to stream - apply connections first');
      return;
    }

    try {
      // Start streaming via REST API
      const resp = await fetch('/api/stream/configure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection_count: connections.size,
          active: true
        })
      });

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      isStreaming = true;
      updateStreamingUI();

      // Connect WebSocket for real-time updates
      connectWebSocket();

      console.log('🚀 Streaming started');
    } catch (err) {
      console.error('Failed to start streaming:', err);
    }
  }

  async function stopStreaming() {
    try {
      // Stop streaming via REST API
      const resp = await fetch('/api/stream/configure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection_count: connections.size,
          active: false
        })
      });

      isStreaming = false;
      updateStreamingUI();

      // Disconnect WebSocket
      if (ws) {
        ws.close();
        ws = null;
      }

      // Update all connections to idle
      connections.forEach((conn) => {
        conn.status = 'idle';
      });
      updateDeviceList();

      console.log('⏹ Streaming stopped');
    } catch (err) {
      console.error('Failed to stop streaming:', err);
    }
  }

  function updateStreamingUI() {
    const btn = $('#btnStream');
    if (isStreaming) {
      btn.classList.add('active');
      btn.querySelector('.btn-icon').textContent = '⏹';
      btn.querySelector('.btn-text').textContent = 'Stop';
      $('#streamIndicator').classList.add('show');
    } else {
      btn.classList.remove('active');
      btn.querySelector('.btn-icon').textContent = '▶';
      btn.querySelector('.btn-text').textContent = 'Start';
      $('#streamIndicator').classList.remove('show');
    }
  }

  function connectWebSocket() {
    if (ws && ws.readyState === WebSocket.OPEN) return;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/stream`;

    try {
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        console.log('✅ WebSocket connected');
        // Update all connections to streaming
        connections.forEach((conn) => {
          conn.status = 'streaming';
        });
        updateDeviceList();
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);

          if (msg.type === 'sensor_update' && msg.data) {
            // Update connection data
            Object.entries(msg.data).forEach(([connId, payload]) => {
              const conn = connections.get(connId);
              if (conn) {
                conn.data = payload;
                conn.status = 'streaming';
              }
            });

            // Update published count - msg.count is how many were published this tick
            if (msg.count) {
              publishedCount += msg.count;
              $('#publishedCount').textContent = publishedCount.toLocaleString();
            }

            // Update selected device detail
            if (selectedDeviceId && msg.data[selectedDeviceId]) {
              updateDeviceDetail(msg.data[selectedDeviceId]);
            }
          } else if (msg.type === 'init') {
            // Initial state from server
            if (msg.data) {
              Object.entries(msg.data).forEach(([connId, payload]) => {
                const conn = connections.get(connId);
                if (conn) {
                  conn.data = payload;
                }
              });
            }
          }
        } catch (e) {
          console.error('WebSocket message parse error:', e);
        }
      };

      ws.onerror = (err) => {
        console.error('WebSocket error:', err);
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected');
        connections.forEach((conn) => {
          conn.status = 'idle';
        });
        updateDeviceList();
      };
    } catch (err) {
      console.error('Failed to connect WebSocket:', err);
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // EXPORTS
  // ══════════════════════════════════════════════════════════════════════════
  document.addEventListener('DOMContentLoaded', init);

  return {
    toggleStreaming,
    applyConnectionCount,
    selectDevice
  };
})();
