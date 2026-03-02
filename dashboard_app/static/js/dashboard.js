/* ═══════════════════════════════════════════════════════════════════════════
   Lakebase Dashboard - JavaScript
═══════════════════════════════════════════════════════════════════════════ */

const Dashboard = (() => {
  // State
  let map = null;
  let markerCluster = null;
  let markers = new Map(); // connection_id -> marker
  let clientTrack = null;  // Polyline for selected client
  let trackMarkers = [];   // Start/end markers for track
  let ws = null;
  let currentLayer = 'satellite';
  let selectedClientId = null;
  let currentTrackData = [];      // Full track data for selected client
  let selectedPointIndex = null;  // Currently selected track point index
  let selectedPointRing = null;   // Highlight ring around selected point

  // DOM helpers
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // ══════════════════════════════════════════════════════════════════════════
  // INITIALIZATION
  // ══════════════════════════════════════════════════════════════════════════
  function init() {
    initMap();
    connectWebSocket();
    fetchInitialData();
    setupEventListeners();
    console.log('✅ Lakebase Dashboard initialized');
  }

  function setupEventListeners() {
    const btnClose = $('#btnCloseDetail');
    if (btnClose) {
      btnClose.addEventListener('click', closeDetailPanel);
    }
  }

  function initMap() {
    map = L.map('worldMap', {
      center: [20, 0],
      zoom: 2,
      zoomControl: false,
      minZoom: 2,
      maxZoom: 19
    });

    // Create custom pane for track elements (above markers)
    map.createPane('trackPane');
    map.getPane('trackPane').style.zIndex = 650;
    map.createPane('trackMarkerPane');
    map.getPane('trackMarkerPane').style.zIndex = 660;

    // Use CartoDB Voyager (same as mobile app - clean light map)
    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; CartoDB',
      maxZoom: 19
    }).addTo(map);

    // Zoom control bottom-left
    L.control.zoom({ position: 'bottomleft' }).addTo(map);

    // Marker cluster group
    markerCluster = L.markerClusterGroup({
      showCoverageOnHover: false,
      maxClusterRadius: 50,
      iconCreateFunction: (cluster) => {
        const count = cluster.getChildCount();
        return L.divIcon({
          html: `<div style="
            background:#3b82f6;
            color:#fff;
            border-radius:50%;
            width:36px;
            height:36px;
            display:flex;
            align-items:center;
            justify-content:center;
            font-weight:600;
            font-size:13px;
            border:3px solid rgba(255,255,255,0.8);
            box-shadow:0 2px 8px rgba(0,0,0,0.3);
          ">${count}</div>`,
          className: 'custom-cluster',
          iconSize: [36, 36]
        });
      }
    });
    map.addLayer(markerCluster);
  }

  function setMapLayer(layer) {
    // For now just satellite - can add street layer later
    currentLayer = layer;
    $('#btnSatellite').classList.toggle('active', layer === 'satellite');
  }

  // ══════════════════════════════════════════════════════════════════════════
  // WEBSOCKET
  // ══════════════════════════════════════════════════════════════════════════
  function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/dashboard`;

    try {
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        console.log('✅ WebSocket connected');
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === 'dashboard_update') {
            updateDashboard(msg);
          }
        } catch (e) {
          console.error('WS parse error:', e);
        }
      };

      ws.onerror = (err) => {
        console.error('WebSocket error:', err);
      };

      ws.onclose = () => {
        console.log('WebSocket disconnected, reconnecting in 3s...');
        setTimeout(connectWebSocket, 3000);
      };
    } catch (err) {
      console.error('WebSocket connection failed:', err);
      setTimeout(connectWebSocket, 3000);
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // DATA FETCHING
  // ══════════════════════════════════════════════════════════════════════════
  async function fetchInitialData() {
    try {
      const [summaryRes, clientsRes, locationsRes] = await Promise.all([
        fetch('/api/dashboard/summary'),
        fetch('/api/dashboard/clients'),
        fetch('/api/dashboard/locations')
      ]);

      const summary = await summaryRes.json();
      const clientsData = await clientsRes.json();
      const locationsData = await locationsRes.json();

      updateKPIs(summary);
      updateClientsList(clientsData.clients || []);
      updateMapMarkers(locationsData.locations || []);
      updateAccessTime(locationsData.elapsed_ms);

    } catch (err) {
      console.error('Failed to fetch initial data:', err);
    }
  }

  async function fetchClientDetail(connectionId) {
    try {
      // Request up to 500 track points for detailed path visualization
      const res = await fetch(`/api/dashboard/client/${encodeURIComponent(connectionId)}?track_limit=500`);
      if (!res.ok) throw new Error('Client not found');
      const data = await res.json();
      console.log(`Fetched client ${connectionId}: ${data.track_count || 0} track points`);
      return data;
    } catch (err) {
      console.error('Failed to fetch client detail:', err);
      return null;
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // UI UPDATES
  // ══════════════════════════════════════════════════════════════════════════
  function updateDashboard(msg) {
    if (msg.summary) updateKPIs(msg.summary);
    if (msg.clients) updateClientsList(msg.clients);
    if (msg.locations) updateMapMarkers(msg.locations);

    // Update display time
    const now = new Date();
    $('#kpiDisplayUpdated').textContent = now.toLocaleTimeString('en-GB');
  }

  function updateKPIs(summary) {
    // Total events
    const totalEvents = summary.total_events || 0;
    $('#kpiTotalEvents').textContent = totalEvents.toLocaleString();

    // Unique clients
    const uniqueClients = summary.unique_clients || 0;
    $('#kpiUniqueClients').textContent = uniqueClients.toLocaleString();

    // Data ingested
    const dataBytes = summary.total_payload_bytes || 0;
    const dataMB = (dataBytes / (1024 * 1024)).toFixed(2);
    $('#kpiDataIngested').textContent = dataBytes > 1024*1024 ? `${dataMB} MB` : `${(dataBytes / 1024).toFixed(1)} KB`;

    // Last data timestamp
    if (summary.last_event_time) {
      const lastTime = new Date(summary.last_event_time);
      $('#kpiLastData').textContent = lastTime.toLocaleTimeString('en-GB');
    }
  }

  function updateAccessTime(ms) {
    if (ms !== undefined) {
      $('#kpiAccessTime').textContent = `${ms} ms`;
    }
  }

  function updateClientsList(clients) {
    const listEl = $('#clientsList');
    if (!clients || clients.length === 0) {
      listEl.innerHTML = `
        <div class="clients-loading">
          <span>No clients found</span>
          <span style="font-size:11px;color:#888;margin-top:4px;">Start the mobile app to generate data</span>
        </div>
      `;
      return;
    }

    // Sort: active first, then by event count
    clients.sort((a, b) => {
      if (a.is_active !== b.is_active) return b.is_active ? 1 : -1;
      return (b.event_count || 0) - (a.event_count || 0);
    });

    listEl.innerHTML = clients.map(client => {
      const isActive = client.is_active;
      const isSelected = client.connection_id === selectedClientId;
      const lastSeen = client.last_event_time 
        ? formatRelativeTime(new Date(client.last_event_time))
        : 'Never';

      return `
        <div class="client-card ${isActive ? 'active' : 'inactive'} ${isSelected ? 'selected' : ''}"
             data-connection-id="${client.connection_id}"
             onclick="Dashboard.selectClient('${client.connection_id}')">
          <div class="client-header">
            <span class="client-name">${client.device_name || client.connection_id.substring(0,12)}</span>
            <span class="client-badge ${isActive ? 'active' : 'inactive'}">
              ${isActive ? 'Active' : 'Inactive'}
            </span>
          </div>
          <div class="client-meta">
            ${(client.event_count || 0).toLocaleString()} events • Last: ${lastSeen}
          </div>
        </div>
      `;
    }).join('');
  }

  function updateMapMarkers(locations) {
    if (!locations || locations.length === 0) return;

    // Clear existing markers
    markerCluster.clearLayers();
    markers.clear();

    locations.forEach(loc => {
      if (!loc.latitude || !loc.longitude) return;

      const isActive = loc.is_active;
      const isSelected = loc.connection_id === selectedClientId;
      const color = isSelected ? '#3b82f6' : (isActive ? '#22c55e' : '#f59e0b');

      const marker = L.circleMarker([loc.latitude, loc.longitude], {
        radius: isSelected ? 10 : 8,
        fillColor: color,
        fillOpacity: 1,
        color: '#fff',
        weight: isSelected ? 3 : 2
      });

      marker.bindPopup(`
        <div style="font-family:Inter,sans-serif;min-width:150px;">
          <div style="font-weight:600;margin-bottom:4px;">${loc.device_name || loc.connection_id}</div>
          <div style="font-size:12px;color:#666;">
            Lat: ${loc.latitude.toFixed(5)}<br>
            Lon: ${loc.longitude.toFixed(5)}<br>
            Events: ${(loc.event_count || 0).toLocaleString()}
          </div>
          <button onclick="Dashboard.selectClient('${loc.connection_id}')" 
                  style="margin-top:8px;width:100%;padding:6px;background:#3b82f6;color:#fff;border:none;border-radius:4px;cursor:pointer;font-weight:500;">
            View Details
          </button>
        </div>
      `);

      markerCluster.addLayer(marker);
      markers.set(loc.connection_id, marker);
    });
  }

  // ══════════════════════════════════════════════════════════════════════════
  // CLIENT SELECTION & DETAIL
  // ══════════════════════════════════════════════════════════════════════════
  async function selectClient(connectionId) {
    selectedClientId = connectionId;
    
    // Update card selection state
    $$('.client-card').forEach(card => {
      card.classList.toggle('selected', card.dataset.connectionId === connectionId);
    });

    // Show loading state
    const panel = $('#clientDetailPanel');
    panel.style.display = 'block';
    $('#mainContent').classList.add('client-selected');
    // Let Leaflet know the map container changed size
    setTimeout(() => map.invalidateSize({ animate: true }), 50);
    $('#detailDeviceName').textContent = 'Loading...';
    
    // Fetch client details
    const data = await fetchClientDetail(connectionId);
    if (!data || !data.summary) {
      panel.style.display = 'none';
      return;
    }

    const s = data.summary;
    
    // Update detail panel
    $('#detailDeviceName').textContent = s.device_name || connectionId.substring(0, 16);
    
    const statusEl = $('#detailStatus');
    statusEl.textContent = s.is_active ? 'Active' : 'Inactive';
    statusEl.className = 'detail-status ' + (s.is_active ? 'active' : 'inactive');
    
    $('#detailTotalEvents').textContent = (s.total_events || 0).toLocaleString();
    
    const totalBytes = s.total_bytes || 0;
    const sizeStr = totalBytes > 1024*1024 
      ? `${(totalBytes / (1024*1024)).toFixed(2)} MB`
      : `${(totalBytes / 1024).toFixed(1)} KB`;
    $('#detailDataIngested').textContent = sizeStr;
    
    $('#detailFirstSeen').textContent = s.first_event 
      ? new Date(s.first_event).toLocaleString('en-GB', {day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'})
      : '--';
    $('#detailLastSeen').textContent = s.last_event 
      ? formatRelativeTime(new Date(s.last_event))
      : '--';

    // Draw track on map and zoom to client location
    if (data.track && data.track.length > 0) {
      drawClientTrack(data.track, s.latest);
      $('#detailTrackPoints').textContent = `${data.track.length} track points drawn on map`;
    } else if (s.latest && (s.latest.latitude || s.latest.lat)) {
      // No track but have latest position - zoom to it
      clearClientTrack();
      const lat = s.latest.latitude || s.latest.lat;
      const lng = s.latest.longitude || s.latest.lng || s.latest.lon;
      map.setView([lat, lng], 14);
      
      // Add a single marker for current position
      const posMarker = L.circleMarker([lat, lng], {
        radius: 10, fillColor: '#3b82f6', fillOpacity: 1, color: '#fff', weight: 3
      }).addTo(map);
      posMarker.bindTooltip('Current Position', {permanent: false, direction: 'top'});
      trackMarkers.push(posMarker);
      
      $('#detailTrackPoints').textContent = 'Single location point (no track history)';
    } else {
      clearClientTrack();
      $('#detailTrackPoints').textContent = 'No location data available';
    }
  }

  function drawClientTrack(track, latest) {
    clearClientTrack();
    currentTrackData = track;  // Store for point click lookup
    selectedPointIndex = null;
    
    // Handle both 'lon' and 'lng' field names for compatibility
    const points = track
      .filter(p => p.lat && (p.lon || p.lng))
      .map(p => [p.lat, p.lon || p.lng]);
    
    console.log(`Drawing track with ${points.length} points`, points.slice(0, 3));
    
    if (points.length === 0) {
      if (latest && (latest.latitude || latest.lat)) {
        const lat = latest.latitude || latest.lat;
        const lng = latest.longitude || latest.lng || latest.lon;
        map.setView([lat, lng], 14);
      }
      return;
    }

    // Draw connecting line between points (thin grey line)
    clientTrack = L.polyline(points, {
      color: '#94a3b8',
      weight: 2,
      opacity: 0.6,
      lineJoin: 'round',
      lineCap: 'round',
      pane: 'trackPane'
    }).addTo(map);

    // Draw a blob (circle) for each track point — clickable
    const totalPoints = points.length;
    points.forEach((point, index) => {
      const progress = index / Math.max(totalPoints - 1, 1);
      const radius = 4 + (progress * 4);
      
      const r = Math.round(59 + progress * (239 - 59));
      const g = Math.round(130 + progress * (68 - 130));
      const b = Math.round(246 + progress * (68 - 246));
      const color = `rgb(${r}, ${g}, ${b})`;
      
      const blob = L.circleMarker(point, {
        radius: radius,
        fillColor: color,
        fillOpacity: 0.8,
        color: '#fff',
        weight: 2,
        pane: 'trackMarkerPane'
      }).addTo(map);
      
      blob.bindTooltip(`Point ${index + 1} of ${totalPoints} — click to inspect`, {
        permanent: false,
        direction: 'top'
      });
      
      // Click handler — show this point's data in the detail panel
      blob.on('click', () => selectTrackPoint(index));
      
      trackMarkers.push(blob);
    });

    // START marker
    const startIcon = L.divIcon({
      className: 'track-start-marker',
      html: '<div class="start-marker">S</div>',
      iconSize: [28, 28],
      iconAnchor: [14, 14]
    });
    const startMarker = L.marker(points[0], { icon: startIcon, pane: 'trackMarkerPane', zIndexOffset: 1000 }).addTo(map);
    startMarker.on('click', () => selectTrackPoint(0));
    trackMarkers.push(startMarker);

    // END marker
    const endIcon = L.divIcon({
      className: 'track-end-marker',
      html: '<div class="end-marker">E</div>',
      iconSize: [32, 32],
      iconAnchor: [16, 16]
    });
    const lastPoint = points[points.length - 1];
    const endMarker = L.marker(lastPoint, { icon: endIcon, pane: 'trackMarkerPane', zIndexOffset: 2000 }).addTo(map);
    endMarker.on('click', () => selectTrackPoint(points.length - 1));
    trackMarkers.push(endMarker);

    // Zoom to fit
    map.invalidateSize({ animate: false });
    setTimeout(() => {
      map.fitBounds(clientTrack.getBounds(), { 
        padding: [60, 60], 
        maxZoom: 17,
        animate: true
      });
    }, 100);
    
    // Show summary in left stats area initially
    showClientSummaryInStats();
    
    console.log(`✅ Track drawn: ${points.length} clickable points, start:`, points[0], 'end:', lastPoint);
  }

  // ── Point selection ─────────────────────────────────────────────────────
  function selectTrackPoint(index) {
    if (index < 0 || index >= currentTrackData.length) return;
    selectedPointIndex = index;
    const pt = currentTrackData[index];
    const total = currentTrackData.length;
    
    // Update point badge
    $('#detailPointBadge').style.display = 'inline';
    $('#detailPointNum').textContent = index + 1;
    $('#detailPointTotal').textContent = total;
    
    // Update left stats with point-level data
    const ts = pt.event_time
      ? new Date(pt.event_time).toLocaleString('en-GB', {
          day:'2-digit', month:'short', hour:'2-digit', minute:'2-digit', second:'2-digit'
        })
      : '--';
    $('#detailTimestamp').textContent = ts;
    $('#detailLat').textContent = pt.lat ? pt.lat.toFixed(6) : '--';
    $('#detailLng').textContent = (pt.lon || pt.lng) ? (pt.lon || pt.lng).toFixed(6) : '--';
    $('#detailSpeed').textContent = `${(pt.speed_kmh || 0).toFixed(1)} km/h`;
    $('#detailHeading').textContent = `${(pt.heading_deg || 0).toFixed(0)}°`;
    $('#detailBattery').textContent = `${pt.battery_pct || 0}%`;
    
    // Hide the hint
    const hint = $('#pointHint');
    if (hint) hint.style.display = 'none';
    
    // Highlight selected point on map
    highlightPoint(index);
    
    console.log(`📍 Selected point ${index + 1}/${total}:`, pt);
  }

  function highlightPoint(index) {
    // Remove old highlight
    if (selectedPointRing) {
      map.removeLayer(selectedPointRing);
      selectedPointRing = null;
    }
    
    const pt = currentTrackData[index];
    if (!pt) return;
    const lat = pt.lat;
    const lng = pt.lon || pt.lng;
    if (!lat || !lng) return;

    // Draw a bright ring around selected point
    selectedPointRing = L.circleMarker([lat, lng], {
      radius: 14,
      fillColor: 'transparent',
      fillOpacity: 0,
      color: '#3b82f6',
      weight: 3,
      opacity: 1,
      pane: 'trackMarkerPane',
      className: 'selected-point-ring'
    }).addTo(map);
    
    // Pan map to the point smoothly
    map.panTo([lat, lng], { animate: true, duration: 0.4 });
  }

  function showClientSummaryInStats() {
    // Show placeholder prompting user to click a point
    $('#detailTimestamp').textContent = '--';
    $('#detailLat').textContent = '--';
    $('#detailLng').textContent = '--';
    $('#detailSpeed').textContent = '--';
    $('#detailHeading').textContent = '--';
    $('#detailBattery').textContent = '--';
    $('#detailPointBadge').style.display = 'none';
    const hint = $('#pointHint');
    if (hint) hint.style.display = 'block';
  }

  function clearClientTrack() {
    if (clientTrack) { map.removeLayer(clientTrack); clientTrack = null; }
    trackMarkers.forEach(m => map.removeLayer(m));
    trackMarkers = [];
    currentTrackData = [];
    selectedPointIndex = null;
    if (selectedPointRing) { map.removeLayer(selectedPointRing); selectedPointRing = null; }
  }

  function closeDetailPanel() {
    selectedClientId = null;
    $('#clientDetailPanel').style.display = 'none';
    $('#mainContent').classList.remove('client-selected');
    clearClientTrack();
    $$('.client-card').forEach(card => card.classList.remove('selected'));
    // Re-invalidate map after panel closes so it fills the space
    setTimeout(() => map.invalidateSize({ animate: true }), 50);
    fetchInitialData();
  }

  // ══════════════════════════════════════════════════════════════════════════
  // HELPERS
  // ══════════════════════════════════════════════════════════════════════════
  function formatRelativeTime(date) {
    const now = new Date();
    const diffMs = now - date;
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);

    if (diffSec < 10) return 'Just now';
    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffMin < 60) return `${diffMin} min ago`;
    if (diffHour < 24) return `${diffHour} hours ago`;
    return date.toLocaleDateString();
  }

  // ══════════════════════════════════════════════════════════════════════════
  // INIT
  // ══════════════════════════════════════════════════════════════════════════
  document.addEventListener('DOMContentLoaded', init);

  return {
    setMapLayer,
    selectClient,
    closeDetailPanel
  };
})();
