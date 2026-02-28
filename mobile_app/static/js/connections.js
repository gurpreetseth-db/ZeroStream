/**
 * ZeroStream - Connection Grid Manager
 * Renders and updates the device connection grid cards.
 */

const Connections = (() => {
  // ── State ──────────────────────────────────────────────────────────────
  let _connections  = {};   // connection_id → latest payload
  let _openModalId  = null;
  let _cardElements = {};   // connection_id → DOM element

  // ── Render grid ────────────────────────────────────────────────────────
  function renderGrid(connectionIds) {
    const grid = document.getElementById('connectionsGrid');

    // Add new cards
    connectionIds.forEach(cid => {
      if (!_cardElements[cid]) {
        const card = _createCard(cid);
        grid.appendChild(card);
        _cardElements[cid] = card;
      }
    });

    // Remove stale cards
    Object.keys(_cardElements).forEach(cid => {
      if (!connectionIds.includes(cid)) {
        _cardElements[cid].remove();
        delete _cardElements[cid];
      }
    });

    // Remove empty state
    const empty = grid.querySelector('.empty-state');
    if (empty && connectionIds.length > 0) empty.remove();

    // Show empty state if no connections
    if (connectionIds.length === 0 && !grid.querySelector('.empty-state')) {
      grid.innerHTML = `
        <div class="empty-state">
          <div class="empty-icon">📱</div>
          <div class="empty-text">Use the slider above to add connections</div>
        </div>`;
      _cardElements = {};
    }

    // Update active count badge
    document.getElementById('activeCount').textContent = connectionIds.length;
  }

  function _createCard(cid) {
    const card = document.createElement('div');
    card.className   = 'connection-card inactive';
    card.dataset.cid = cid;
    card.innerHTML   = `
      <div class="card-status-dot"></div>
      <div class="card-icon">📱</div>
      <div class="card-name" id="name-${cid}">...</div>
      <div class="card-city" id="city-${cid}">...</div>
      <div class="card-events" id="events-${cid}">0 events</div>
    `;
    card.addEventListener('click', () => openPhoneModal(cid));
    return card;
  }

  // ── Update all connections from WS payload ─────────────────────────────
  function updateAll(dataMap) {
    const ids = Object.keys(dataMap);

    // Ensure grid has cards for all connections
    renderGrid(ids);

    ids.forEach(cid => {
      const payload = dataMap[cid];
      _connections[cid] = payload;
      _updateCard(cid, payload);
    });
  }

  function _updateCard(cid, payload) {
    const card = _cardElements[cid];
    if (!card) return;

    // Active state
    card.classList.remove('active', 'inactive');
    card.classList.add('active');

    // Text fields
    const nameEl   = document.getElementById(`name-${cid}`);
    const cityEl   = document.getElementById(`city-${cid}`);
    const eventsEl = document.getElementById(`events-${cid}`);

    if (nameEl)   nameEl.textContent   = payload.device_name || cid;
    if (eventsEl) eventsEl.textContent = `${(payload._event_count || 0)} ev`;

    // Flash animation on update
    card.style.boxShadow = '0 0 16px rgba(63,185,80,0.4)';
    setTimeout(() => {
      card.style.boxShadow = '';
    }, 400);
  }

  function markAllInactive() {
    Object.values(_cardElements).forEach(card => {
      card.classList.remove('active');
      card.classList.add('inactive');
    });
  }

  // ── Modal ──────────────────────────────────────────────────────────────
  function openPhoneModal(cid) {
    _openModalId = cid;
    const payload = _connections[cid];

    if (payload) {
      PhoneSensors.update(payload);
    } else {
      // Fetch from API if not in cache
      fetch(`/api/connections/${cid}`)
        .then(r => r.json())
        .then(data => {
          if (data.latest) PhoneSensors.update(data.latest);
        })
        .catch(console.error);
    }

    // Init phone map
    PhoneMap.init(
      payload?.latitude  || 0,
      payload?.longitude || 0
    );

    document.getElementById('phoneModal').classList.add('open');
  }

  function getOpenModalId()  { return _openModalId; }
  function clearOpenModalId() { _openModalId = null; }

  return {
    updateAll,
    markAllInactive,
    renderGrid,
    getOpenModalId,
    clearOpenModalId,
  };
})();