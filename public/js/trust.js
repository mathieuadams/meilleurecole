// trust.js - Academy Trust summary page

(function() {
  function slugFromPath() {
    const parts = (window.location.pathname || '').split('/').filter(Boolean);
    const idx = parts.indexOf('trust');
    if (idx >= 0 && parts[idx+1]) return parts[idx+1].toLowerCase();
    // Also support /trust.html?trust=slug
    try {
      const p = new URLSearchParams(window.location.search || '');
      const t = p.get('trust');
      if (t) return String(t).toLowerCase();
    } catch {}
    return '';
  }

  function fmt(n) { return n == null ? '-' : Number(n).toLocaleString('en-GB'); }
  function ratingBadge(r) {
    if (r == null || isNaN(r)) return 'badge gray';
    const v = Number(r);
    if (v >= 8) return 'badge green';
    if (v >= 6) return 'badge blue';
    if (v >= 4) return 'badge orange';
    return 'badge red';
  }

  async function loadTrust() {
    const trustSlug = slugFromPath();
    if (!trustSlug) return;
    try {
      const res = await fetch(`/api/trust/${encodeURIComponent(trustSlug)/**/}/summary`);
      const data = await res.json();
      if (!data || !data.success) return;

      const t = data.trust || { name: 'Academy Trust' };
      document.getElementById('trustName').textContent = t.name;

      document.title = `${t.name} | Academy Trust | MeilleureEcole.fr`;
      const desc = `Overview of ${t.name}, including ${data.totalSchools || 0} schools.`;
      const metaDescription = document.getElementById('metaDescription');
      if (metaDescription) metaDescription.setAttribute('content', desc);

      document.getElementById('totalSchools').textContent = fmt(data.totalSchools);
      document.getElementById('totalStudents').textContent = fmt(data.totalStudents);
      document.getElementById('primaryCount').textContent = fmt(data.primaryCount);
      document.getElementById('secondaryCount').textContent = fmt(data.secondaryCount);
      document.getElementById('sixthFormCount').textContent = fmt(data.sixthFormCount);
      document.getElementById('specialCount').textContent = fmt(data.specialCount);

      // List schools
      const list = document.getElementById('trustSchools');
      if (list) {
        const html = (data.schools || []).map(s => {
          const href = window.schoolPath ? window.schoolPath(s) : `/school/${s.urn}`;
          const rating = s.overall_rating != null ? Number(s.overall_rating) : null;
          const ratingDisplay = rating == null ? 'N/A' : (rating >= 10 ? '10' : rating.toFixed(1));
          return `
            <div class="school-item" onclick="window.location.href='${href}'">
              <div>
                <div style="font-weight:600">${s.name}</div>
                <div style="color:#6b7280; font-size:.9rem;">${s.type_of_establishment || ''} — ${s.town || ''} ${s.postcode || ''}</div>
              </div>
              <div class="${ratingBadge(rating)}">${ratingDisplay}/10</div>
            </div>
          `;
        }).join('');
        list.innerHTML = html || '<div style="padding:1rem; color:#6b7280;">No schools found for this trust.</div>';
      }
    } catch (e) {
      console.error('Failed to load trust summary', e);
    }
  }

  // Load header/footer
  async function loadComponents() {
    const comps = ['header','footer'];
    for (const c of comps) {
      try {
        const r = await fetch(`/components/${c}.html`);
        if (r.ok) {
          const html = await r.text();
          const target = document.getElementById(c);
          if (target) {
            target.innerHTML = html;
            if (c === 'header' && typeof window.initializeHeader === 'function') window.initializeHeader();
          }
        }
      } catch {}
    }
  }

  document.addEventListener('DOMContentLoaded', async () => {
    await loadComponents();
    await loadTrust();
  });
})();

