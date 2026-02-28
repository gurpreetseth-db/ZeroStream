/* ═══════════════════════════════════════════════════════════════════════════
   Zerobus View - Raw Stream Data with Auto-Refresh
═══════════════════════════════════════════════════════════════════════════ */

const ZerobusView = (() => {
  // State
  let refreshInterval = null;
  let totalEvents = 0;

  // DOM helpers
  const $ = (sel) => document.querySelector(sel);

  // ══════════════════════════════════════════════════════════════════════════
  // INITIALIZATION
  // ══════════════════════════════════════════════════════════════════════════
  function init() {
    fetchData();
    startAutoRefresh();
    console.log('✅ Zerobus View initialized');
  }

  function startAutoRefresh() {
    // Refresh every 3 seconds
    refreshInterval = setInterval(fetchData, 3000);
  }

  // ══════════════════════════════════════════════════════════════════════════
  // DATA FETCHING
  // ══════════════════════════════════════════════════════════════════════════
  async function fetchData() {
    showLoading(true);

    try {
      const response = await fetch('/api/zerobus/stream?limit=100');
      const data = await response.json();

      updateStats(data);
      renderTable(data.rows || []);
      updateAccessTime(data.elapsed_ms);

    } catch (err) {
      console.error('Failed to fetch data:', err);
    } finally {
      showLoading(false);
    }
  }

  function showLoading(show) {
    const indicator = $('#loadingIndicator');
    if (indicator) {
      indicator.classList.toggle('show', show);
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // UI UPDATES
  // ══════════════════════════════════════════════════════════════════════════
  function updateStats(data) {
    // Total events
    totalEvents = data.total || data.count || 0;
    $('#statTotalEvents').textContent = totalEvents.toLocaleString();

    // Last updated
    const now = new Date();
    $('#statLastUpdated').textContent = now.toLocaleTimeString('en-GB');
  }

  function updateAccessTime(ms) {
    if (ms !== undefined) {
      $('#statAccessTime').textContent = `${ms} ms`;
    }
  }

  function renderTable(rows) {
    const tbody = $('#dataTableBody');
    if (!rows || rows.length === 0) {
      tbody.innerHTML = `
        <tr class="loading-row">
          <td colspan="7">
            <div class="table-loading">
              <span>No data available</span>
            </div>
          </td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = rows.map(row => {
      // Format timestamp - field is 'event_time' from SQL alias
      const timestamp = row.event_time 
        ? row.event_time
        : '—';

      // Get values with defaults
      const clientId = row.device_name || row.connection_id || '—';
      const heading = formatNumber(row.heading_deg, 0) + '°';
      const pitch = formatNumber(row.pitch_deg, 1);
      const roll = formatNumber(row.roll_deg, 1);
      const lat = formatNumber(row.latitude, 6);
      const lon = formatNumber(row.longitude, 6);

      return `
        <tr>
          <td><a href="#" class="client-link">${escapeHtml(clientId)}</a></td>
          <td>${timestamp}</td>
          <td class="col-heading cell-heading">${heading}</td>
          <td class="col-pitch cell-pitch">${pitch}</td>
          <td class="col-roll cell-roll">${roll}</td>
          <td class="col-lat cell-lat">${lat}</td>
          <td class="col-lon cell-lon">${lon}</td>
        </tr>
      `;
    }).join('');
  }

  // ══════════════════════════════════════════════════════════════════════════
  // HELPERS
  // ══════════════════════════════════════════════════════════════════════════
  function formatTimestamp(isoString) {
    try {
      const date = new Date(isoString);
      const month = date.toLocaleString('en-US', { month: 'short' });
      const day = date.getDate();
      const time = date.toLocaleTimeString('en-GB', { 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit' 
      });
      return `${month} ${day}, ${time}`;
    } catch (e) {
      return isoString;
    }
  }

  function formatNumber(value, decimals) {
    if (value === null || value === undefined) return '—';
    const num = parseFloat(value);
    if (isNaN(num)) return '—';
    return num.toFixed(decimals);
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  // ══════════════════════════════════════════════════════════════════════════
  // INIT
  // ══════════════════════════════════════════════════════════════════════════
  document.addEventListener('DOMContentLoaded', init);

  return {
    refresh: fetchData
  };
})();
