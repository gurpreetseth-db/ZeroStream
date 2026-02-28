/**
 * ZeroStream - Phone Modal Map (Leaflet)
 * Shows current device location with trail.
 */

const PhoneMap = (() => {
  let _map        = null;
  let _marker     = null;
  let _trail      = [];
  let _polyline   = null;
  const MAX_TRAIL = 50;

  // Dark tile layer - Carto Dark Matter (no API key needed)
  const TILE_URL =
    'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png';
  const TILE_ATTR =
    '&copy; <a href="https://carto.com/">CARTO</a>';

  function init(lat, lon) {
    // Destroy previous instance
    destroy();

    const container = document.getElementById('phoneMap');
    if (!container) return;

    _map = L.map('phoneMap', {
      center:          [lat || 0, lon || 0],
      zoom:            10,
      zoomControl:     true,
      attributionControl: false,
      dragging:        true,
      scrollWheelZoom: false,
    });

    L.tileLayer(TILE_URL, {
      attribution: TILE_ATTR,
      subdomains:  'abcd',
      maxZoom:     19,
    }).addTo(_map);

    // Custom marker icon
    const icon = L.divIcon({
      className: '',
      html: `<div style="
        width:14px; height:14px;
        background:#3fb950;
        border:2px solid #fff;
        border-radius:50%;
        box-shadow:0 0 8px #3fb950;
      "></div>`,
      iconSize:   [14, 14],
      iconAnchor: [7, 7],
    });

    _marker = L.marker([lat || 0, lon || 0], { icon }).addTo(_map);
    _trail  = [[lat || 0, lon || 0]];

    _polyline = L.polyline(_trail, {
      color:   '#3b82f6',
      weight:  2,
      opacity: 0.7,
    }).addTo(_map);
  }

  function updateMarker(lat, lon) {
    if (!_map || !_marker) return;

    _marker.setLatLng([lat, lon]);

    // Update trail
    _trail.push([lat, lon]);
    if (_trail.length > MAX_TRAIL) _trail.shift();
    if (_polyline) _polyline.setLatLngs(_trail);

    // Pan map to follow marker
    _map.panTo([lat, lon], { animate: true, duration: 0.5 });
  }

  function destroy() {
    if (_map) {
      _map.remove();
      _map      = null;
      _marker   = null;
      _polyline = null;
      _trail    = [];
    }
  }

  return { init, updateMarker, destroy };
})();