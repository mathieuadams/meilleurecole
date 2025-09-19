// schoolComponents.js - Reusable components for school pages

const SchoolComponents = {
  // School Header Component - displays name, rating, basic info
  renderSchoolHeader: function(school) {
    return `
      <section class="profile-header">
        <div class="container">
          <div class="profile-header-content">
            <div>
              <h1 id="schoolName">${school.name || 'Loading...'}</h1>
              <div class="school-type-info">
                <span id="schoolType">${school.type || '-'}</span><span>•</span>
                <span id="schoolPhase">${school.phase || '-'}</span><span>•</span>
                <span id="ageRange">Ages ${school.characteristics?.age_range || '-'}</span>
              </div>
              <div class="school-address-info">
                <svg width="16" height="16" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/></svg>
                <span id="schoolAddress">${this.formatAddress(school.address) || '-'}</span>
              </div>
            </div>
            <div class="rating-section">
              <div class="main-rating">
                <div class="main-rating-score" id="overallRating">${school.overall_rating ? school.overall_rating + '/10' : '-'}</div>
                <div class="main-rating-label">Overall Rating</div>
              </div>
              ${school.ofsted ? `
              <div class="ofsted-rating">
                <div class="ofsted-score" id="ofstedRating">${school.ofsted.overall_label || '-'}</div>
                <div style="font-size:.75rem;color:#6b7280;">Ofsted</div>
                <div style="font-size:.75rem;color:#9ca3af;" id="ofstedDate">
                  ${school.ofsted.inspection_date ? new Date(school.ofsted.inspection_date).toLocaleDateString() : '-'}
                </div>
              </div>` : ''}
            </div>
          </div>
        </div>
      </section>
    `;
  },

  // Key Stats Bar Component
  renderKeyStats: function(school) {
    const stats = this.calculateKeyStats(school);
    return `
      <section class="key-stats-bar">
        <div class="container">
          <div class="key-stats-grid">
            ${stats.map(stat => `
              <div class="key-stat">
                <div class="key-stat-value">${stat.value}</div>
                <div class="key-stat-label">${stat.label}</div>
              </div>
            `).join('')}
          </div>
        </div>
      </section>
    `;
  },

  // Test Scores Component
  renderTestScores: function(school) {
    if (!school.test_scores) return '';
    
    return `
      <div class="section-card">
        <div class="section-header">
          <h2 class="section-title">Test Scores</h2>
          <span style="font-size:.875rem;color:#6b7280;">2023/2024</span>
        </div>
        
        <p style="color:#374151; margin-bottom:1.5rem; font-size:0.875rem;">
          These scores reflect how well students at this school perform on state-required tests.
        </p>
        
        <div class="test-scores-container">
          ${this.renderTestScoreRow('English', school.test_scores.english)}
          ${this.renderTestScoreRow('Math', school.test_scores.math)}
          ${this.renderTestScoreRow('Science', school.test_scores.science)}
        </div>
        
        <div class="average-legend">
          <span class="legend-item">▼ Local Authority Avg.</span>
        </div>
        
        ${this.renderParentTip()}
      </div>
    `;
  },

  // Individual Test Score Row
  renderTestScoreRow: function(subject, scores) {
    const score = scores?.score;
    const average = scores?.average;
    const scorePercent = score !== null ? Math.round(score) : null;
    const avgPercent = average !== null ? Math.round(average) : null;
    
    return `
      <div class="test-score-row">
        <div class="test-subject">
          <span class="subject-name">${subject}</span>
          <button class="info-btn" onclick="SchoolComponents.showSubjectInfo('${subject.toLowerCase()}')" aria-label="More info about ${subject} scores">
            <svg width="16" height="16" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"/>
            </svg>
          </button>
        </div>
        <div class="score-display">
          <span class="score-percentage">${scorePercent !== null ? scorePercent + '%' : 'N/A'}</span>
          <div class="score-bar">
            <div class="score-fill ${this.getPerformanceClass(score)}" 
                 style="width: ${scorePercent || 0}%"></div>
            ${avgPercent !== null ? `
            <div class="average-marker" style="left: ${avgPercent}%">
              <span class="avg-label">▼ ${avgPercent}%</span>
            </div>` : ''}
          </div>
        </div>
        <button class="expand-btn" onclick="SchoolComponents.toggleDetails('${subject.toLowerCase()}')">
          <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
            <path fill-rule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clip-rule="evenodd"/>
          </svg>
        </button>
      </div>
    `;
  },

  // Map Component
  renderMap: function(containerId, school) {
    const lat = parseFloat(school.latitude);
    const lon = parseFloat(school.longitude);
    
    if (!isNaN(lat) && !isNaN(lon)) {
      this.initMap(containerId, lat, lon, school.name);
    } else {
      const address = this.formatAddress(school.address);
      if (address) {
        this.geocodeAndInitMap(containerId, address, school.name);
      }
    }
  },

  initMap: function(containerId, lat, lon, name) {
    const mapContainer = document.getElementById(containerId);
    if (!mapContainer || mapContainer._leaflet_id) return;
    
    const map = L.map(containerId, { 
      scrollWheelZoom: false,
      zoomControl: true 
    }).setView([lat, lon], 15);
    
    if (window.getDefaultTileLayer) {
      window.getDefaultTileLayer(map);
    } else {
      // Fallback if helper is not loaded for some reason
      L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors, © CARTO',
        subdomains: 'abcd',
        maxZoom: 19
      }).addTo(map);
    }
    
    L.marker([lat, lon]).addTo(map).bindPopup(name || 'School');
    
    mapContainer._leaflet_map = map;
    
    setTimeout(() => map.invalidateSize(), 100);
  },

  geocodeAndInitMap: async function(containerId, address, name) {
    try {
      const url = `https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&countrycodes=gb&q=${encodeURIComponent(address)}`;
      const resp = await fetch(url);
      const data = await resp.json();
      
      if (data && data[0]) {
        this.initMap(containerId, parseFloat(data[0].lat), parseFloat(data[0].lon), name);
      }
    } catch (error) {
      console.error('Geocoding failed:', error);
    }
  },

  // Contact Card Component
  renderContactCard: function(school) {
    const address = this.formatAddress(school.address);
    const headteacher = this.formatHeadteacher(school);
    
    return `
      <div class="contact-card">
        <div class="contact-header">
          <div class="contact-address">${address || '—'}</div>
          <div class="contact-la">${school.address?.local_authority || ''}</div>
        </div>

        <div class="contact-row">
          <div class="contact-icon">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M10 9a3 3 0 100-6 3 3 0 000 6zm-7 9a7 7 0 1114 0H3z" clip-rule="evenodd"/>
            </svg>
          </div>
          <div class="contact-content">
            <div class="contact-label">School Leader</div>
            <div class="contact-value">${headteacher}</div>
          </div>
        </div>

        ${this.renderContactRow('Phone', school.telephone, 'tel')}
        ${this.renderContactRow('Website', school.website, 'web')}

        <div class="map-actions">
          <a href="${this.getGoogleMapsLink(school)}" target="_blank" rel="noopener" class="map-link">
            <svg width="16" height="16" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/>
            </svg>
            Open in Google Maps
          </a>
        </div>
      </div>
    `;
  },

  // Helper Methods
  formatAddress: function(address) {
    if (!address) return '';
    const parts = [];
    if (address.street) parts.push(address.street);
    if (address.locality) parts.push(address.locality);
    if (address.town) parts.push(address.town);
    if (address.postcode) parts.push(address.postcode);
    return parts.join(', ');
  },

  formatHeadteacher: function(school) {
    if (school.headteacher_name) {
      return school.headteacher_name + 
        (school.headteacher_job_title ? ` (${school.headteacher_job_title})` : '');
    }
    return school.headteacher_job_title || '—';
  },

  calculateKeyStats: function(school) {
    const stats = [];
    
    stats.push({
      value: school.demographics?.total_students || '-',
      label: 'Students'
    });
    
    const fsm = school.demographics?.fsm_percentage;
    stats.push({
      value: fsm !== null && fsm !== undefined ? fsm + '%' : '-',
      label: 'Free School Meals'
    });
    
    if (school.test_scores?.english?.score !== null) {
      stats.push({
        value: Math.round(school.test_scores.english.score) + '%',
        label: 'English Score'
      });
    }
    
    if (school.test_scores?.math?.score !== null) {
      stats.push({
        value: Math.round(school.test_scores.math.score) + '%',
        label: 'Math Score'
      });
    }
    
    if (school.attendance?.overall_absence_rate != null) {
      const attendance = (100 - school.attendance.overall_absence_rate).toFixed(1);
      stats.push({
        value: attendance + '%',
        label: 'Attendance'
      });
    }
    
    return stats;
  },

  getPerformanceClass: function(score) {
    if (score >= 70) return 'high-performing';
    if (score >= 50) return 'average-performing';
    return 'low-performing';
  },

  renderContactRow: function(label, value, type) {
    const iconPath = type === 'tel' ? 
      'M2 3a1 1 0 011-1h2.153a1 1 0 01.986.836l.74 4.435a1 1 0 01-.54 1.06l-1.548.773a11.037 11.037 0 006.105 6.105l.774-1.548a1 1 0 011.059-.54l4.435.74a1 1 0 01.836.986V17a1 1 0 01-1 1h-2C7.82 18 2 12.18 2 5V3z' :
      'M14.243 5.757a6 6 0 10-.986 9.284 1 1 0 111.087 1.678A8 8 0 1118 10a3 3 0 01-4.8 2.401A4 4 0 1114 10a1 1 0 102 0c0-1.537-.586-3.07-1.757-4.243zM12 10a2 2 0 10-4 0 2 2 0 004 0z';
    
    if (!value) {
      return `
        <div class="contact-row">
          <div class="contact-icon">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="${iconPath}" clip-rule="evenodd"/>
            </svg>
          </div>
          <div class="contact-content">
            <div class="contact-label">${label}</div>
            <div class="contact-value" style="color:#6b7280;">Not available</div>
          </div>
        </div>
      `;
    }
    
    if (type === 'tel') {
      return `
        <div class="contact-row">
          <div class="contact-icon">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
              <path d="${iconPath}"/>
            </svg>
          </div>
          <div class="contact-content">
            <div class="contact-label">${label}</div>
            <a href="tel:${value.replace(/\s+/g,'')}" rel="nofollow">${value}</a>
          </div>
        </div>
      `;
    }
    
    // Website
    let url = value;
    if (!value.startsWith('http://') && !value.startsWith('https://')) {
      url = 'http://' + value;
    }
    let displayText = value;
    try {
      const urlObj = new URL(url);
      displayText = urlObj.host.replace(/^www\./,'');
    } catch {}
    
    return `
      <div class="contact-row">
        <div class="contact-icon">
          <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
            <path fill-rule="evenodd" d="${iconPath}" clip-rule="evenodd"/>
          </svg>
        </div>
        <div class="contact-content">
          <div class="contact-label">${label}</div>
          <a href="${url}" target="_blank" rel="noopener">${displayText}</a>
        </div>
      </div>
    `;
  },

  getGoogleMapsLink: function(school) {
    if (school.latitude && school.longitude) {
      return `https://www.google.com/maps/search/?api=1&query=${school.latitude},${school.longitude}`;
    }
    const address = this.formatAddress(school.address);
    return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(address)}`;
  },

  renderParentTip: function() {
    return `
      <div class="parent-tip-box">
        <div class="tip-icon">🦉</div>
        <div class="tip-content">
          <h4 class="tip-title">Parent Tip</h4>
          <p class="tip-text">
            Even high-performing schools can have disparities between student groups. Understand what 
            <a href="https://www.gov.uk/national-curriculum" target="_blank" style="color:#2563eb;">on-track learning looks like</a> 
            for your child and how you can help at home.
          </p>
        </div>
      </div>
    `;
  },

  // Interactive Methods
  showSubjectInfo: function(subject) {
    const info = {
      english: 'This shows the percentage of students meeting expected standards in English/Reading assessments.',
      math: 'This shows the percentage of students meeting expected standards in Mathematics assessments.',
      science: 'This shows the percentage of students meeting expected standards in Science assessments.'
    };
    alert(info[subject] || 'Information not available');
  },

  toggleDetails: function(subject) {
    const detailed = document.getElementById('detailedPerformance');
    if (detailed) {
      detailed.style.display = detailed.style.display === 'none' ? 'block' : 'none';
    }
  }
};

// Export for use in other files
if (typeof module !== 'undefined' && module.exports) {
  module.exports = SchoolComponents;
}
