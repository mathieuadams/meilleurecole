// city.js - Complete JavaScript for City Page with fair ranking system and Scotland support

// Global variables
let cityData = {
  name: '',
  schools: [],
  localAuthorities: {},
  schoolsByPhase: {
    primary: [],
    secondary: [],
    sixthForm: []
  },
  isScottish: false, // Add flag for Scottish cities (legacy)
  isNonEngland: false,
  country: 'England'
};

function canonicalUrl() {
  const trimmedPath = window.location.pathname.endsWith('/') && window.location.pathname !== '/'
    ? window.location.pathname.slice(0, -1)
    : window.location.pathname;
  return `https://www.findschool.uk${trimmedPath || '/'}`;
}

function updateCityMeta(cityName) {
  const pageTitle = `${cityName} Schools Guide | FindSchool.uk`;
  const description = `Explore top performing schools in ${cityName} with FindSchool.uk. Compare Ofsted ratings, academic results and parent reviews to choose confidently.`;
  const url = canonicalUrl();

  document.title = pageTitle;
  const titleEl = document.getElementById('pageTitle');
  if (titleEl) titleEl.textContent = pageTitle;

  const metaDescription = document.getElementById('metaDescription');
  if (metaDescription) metaDescription.setAttribute('content', description);

  const canonicalLink = document.getElementById('canonicalLink');
  if (canonicalLink) canonicalLink.setAttribute('href', url);

  const alternateLink = document.getElementById('alternateLink');
  if (alternateLink) alternateLink.setAttribute('href', url);

  const ogTitle = document.getElementById('ogTitle');
  if (ogTitle) ogTitle.setAttribute('content', pageTitle);

  const ogDescription = document.getElementById('ogDescription');
  if (ogDescription) ogDescription.setAttribute('content', description);

  const ogUrl = document.getElementById('ogUrl');
  if (ogUrl) ogUrl.setAttribute('content', url);

  const twitterTitle = document.getElementById('twitterTitle');
  if (twitterTitle) twitterTitle.setAttribute('content', pageTitle);

  const twitterDescription = document.getElementById('twitterDescription');
  if (twitterDescription) twitterDescription.setAttribute('content', description);

  const structuredDataEl = document.getElementById('structuredData');
  if (structuredDataEl) {
    const structuredData = {
      "@context": "https://schema.org",
      "@graph": [
        {
          "@type": "CollectionPage",
          "name": pageTitle,
          "url": url,
          "description": description,
          "isPartOf": {
            "@type": "WebSite",
            "name": "FindSchool.uk",
            "url": "https://www.findschool.uk/"
          }
        },
        {
          "@type": "BreadcrumbList",
          "itemListElement": [
            {
              "@type": "ListItem",
              "position": 1,
              "name": "Home",
              "item": "https://www.findschool.uk/"
            },
            {
              "@type": "ListItem",
              "position": 2,
              "name": cityName,
              "item": url
            }
          ]
        }
      ]
    };
    structuredDataEl.textContent = JSON.stringify(structuredData, null, 2);
  }
}

// Initialize page
(async function init() {
  const citySlug = window.location.pathname.split('/').filter(Boolean)[0] || '';
  cityData.name = citySlug.split('-').map(word => 
    word.charAt(0).toUpperCase() + word.slice(1)
  ).join(' ');
  
  // Update page titles
  document.getElementById('cityName').textContent = cityData.name;
  document.getElementById('cityNameInLA').textContent = cityData.name;
  document.getElementById('cityNameInRating').textContent = cityData.name;
  document.getElementById('cityNameInSchools').textContent = cityData.name;
  updateCityMeta(cityData.name);
  
  // Wire city search button to localized search page
  const citySearchLink = document.getElementById('citySearchLink');
  if (citySearchLink) {
    const areaParam = encodeURIComponent(cityData.name);
    citySearchLink.href = `/search.html?area=${areaParam}`;
    citySearchLink.setAttribute('aria-label', `Search all schools in ${cityData.name}`);
    // Update visible label as well
    const labelSpan = citySearchLink.querySelector('span:last-child');
    if (labelSpan) {
      labelSpan.textContent = `Search all schools in ${cityData.name}`;
    }
  }
  
  // Load city schools
  await loadCitySchools();
})();

// Get city country from first school's API
async function getCityCountry(schools) {
  if (schools && schools.length > 0) {
    try {
      const firstSchool = schools[0];
      if (firstSchool && firstSchool.urn) {
        const response = await fetch(`/api/schools/${firstSchool.urn}`);
        const data = await response.json();
        return data.school?.country || 'England';
      }
    } catch (error) {
      console.error('Error checking country:', error);
    }
  }
  return 'England';
}

// Hide England-only features (Ofsted, sixth form UI)
function hideEnglandOnlyFeatures() {
  // Hide Ofsted ratings section
  const ratingsSection = document.querySelector('.ratings-section');
  if (ratingsSection) {
    ratingsSection.style.display = 'none';
  }
  
  // Hide sixth form tab
  const sixthFormTab = document.querySelector('[data-phase="sixth-form"]');
  if (sixthFormTab) {
    sixthFormTab.style.display = 'none';
  }
  
  // Hide sixth form count in stats
  const sixthFormStatCard = document.getElementById('sixthFormCount')?.closest('.stat-card');
  if (sixthFormStatCard) {
    sixthFormStatCard.style.display = 'none';
  }
  
  // Adjust grid to 3 columns for remaining stats
  const statsGrid = document.querySelector('.stats-grid');
  if (statsGrid) {
    statsGrid.style.gridTemplateColumns = 'repeat(3, 1fr)';
  }
}

// Load all schools in the city
async function loadCitySchools() {
  try {
    const response = await fetch(`/api/search?q=${encodeURIComponent(cityData.name)}&type=location&limit=500`);
    const data = await response.json();
    
    if (data.success && data.schools) {
      cityData.schools = data.schools;
      
      // Determine country flags
      cityData.country = await getCityCountry(data.schools);
      cityData.isNonEngland = (cityData.country && cityData.country.toLowerCase() !== 'england');
      cityData.isScottish = (cityData.country === 'Scotland');
      if (cityData.isNonEngland) hideEnglandOnlyFeatures();
      
      processSchoolsData();
      renderLocalAuthorities();
      
      // Only render Ofsted distribution for England
      if (!cityData.isNonEngland) {
        renderOfstedDistribution();
      }
      
      await renderTopSchools();
    }
  } catch (error) {
    console.error('Error loading city schools:', error);
  }
}

// Process schools data for statistics
function processSchoolsData() {
  let primaryCount = 0;
  let secondaryCount = 0;
  let sixthFormCount = 0;
  let specialCount = 0;
  let totalStudents = 0;
  
  let ofstedCounts = {
    outstanding: 0,
    good: 0,
    requiresImprovement: 0,
    inadequate: 0,
    notInspected: 0
  };
  
  // Clear arrays
  cityData.schoolsByPhase.primary = [];
  cityData.schoolsByPhase.secondary = [];
  cityData.schoolsByPhase.sixthForm = [];
  cityData.localAuthorities = {};
  
  // Helper to robustly classify school phases across UK (NI included)
  const classify = (school) => {
    const phase = (school.phase_of_education || '').toLowerCase();
    const type = (school.type_of_establishment || '').toLowerCase();
    const group = (school.establishment_group || '').toLowerCase();
    if (type.includes('special') || phase.includes('special') || group.includes('special')) return 'special';
    if (phase.includes('all-through') || phase.includes('through') || type.includes('all-through')) return 'all-through';
    if (phase.includes('primary') || phase.includes('infant') || phase.includes('junior') || phase.includes('first') || type.includes('primary')) return 'primary';
    if (phase.includes('secondary') || phase.includes('middle') || phase.includes('high') || phase.includes('upper') || /post[-\s]?primary/.test(phase) || /post[-\s]?primary/.test(type) || type.includes('secondary') || type.includes('grammar') || type.includes('high school')) return 'secondary';
    if (phase.includes('sixth') || phase.includes('16') || phase.includes('post-16') || type.includes('sixth') || type.includes('post-16')) return 'sixth';
    return null;
  };

  cityData.schools.forEach(school => {
    const phase = (school.phase_of_education || '').toLowerCase();
    const type = (school.type_of_establishment || '').toLowerCase();
    const la = school.local_authority || 'Unknown';
    
    // Group by local authority
    if (!cityData.localAuthorities[la]) {
      cityData.localAuthorities[la] = {
        name: la,
        schools: [],
        primary: 0,
        secondary: 0,
        special: 0,
        outstanding: 0,
        good: 0,
        students: 0
      };
    }
    cityData.localAuthorities[la].schools.push(school);
    
    // Count by phase
    const cls = classify(school);
    if (cls === 'special') {
      specialCount++;
      cityData.localAuthorities[la].special++;
    } else if (cls === 'primary' || cls === 'all-through') {
      primaryCount++;
      cityData.schoolsByPhase.primary.push(school);
      cityData.localAuthorities[la].primary++;
    } else if (cls === 'secondary' || cls === 'all-through') {
      secondaryCount++;
      cityData.schoolsByPhase.secondary.push(school);
      cityData.localAuthorities[la].secondary++;
      
      // Don't count sixth form for Scottish schools
      if (!cityData.isScottish) {
        sixthFormCount++;
        cityData.schoolsByPhase.sixthForm.push(school);
      }
    }
    
    // Count students (fallback to total_pupils for NI)
    const pupils = school.number_on_roll ?? school.total_pupils;
    if (pupils) {
      const students = parseInt(pupils) || 0;
      totalStudents += students;
      cityData.localAuthorities[la].students += students;
    }
    
    // Count Ofsted ratings (England only)
    if (!cityData.isNonEngland) {
      switch(school.ofsted_rating) {
        case 1: 
          ofstedCounts.outstanding++;
          cityData.localAuthorities[la].outstanding++;
          break;
        case 2: 
          ofstedCounts.good++;
          cityData.localAuthorities[la].good++;
          break;
        case 3: 
          ofstedCounts.requiresImprovement++;
          break;
        case 4: 
          ofstedCounts.inadequate++;
          break;
        default: 
          ofstedCounts.notInspected++;
          break;
      }
    }
  });
  
  // Update stats
  document.getElementById('totalSchools').textContent = cityData.schools.length;
  document.getElementById('totalStudents').textContent = formatNumber(totalStudents);
  document.getElementById('totalLAs').textContent = Object.keys(cityData.localAuthorities).length;
  
  document.getElementById('primaryCount').textContent = primaryCount;
  document.getElementById('secondaryCount').textContent = secondaryCount;
  
  // Only show sixth form count for England
  if (!cityData.isNonEngland) {
    document.getElementById('sixthFormCount').textContent = sixthFormCount;
  }
  
  document.getElementById('specialCount').textContent = specialCount;
  
  // Store ofsted counts for later use
  cityData.ofstedCounts = ofstedCounts;
}

// Render local authorities breakdown
function renderLocalAuthorities() {
  const laGrid = document.getElementById('laGrid');
  if (!laGrid) return;
  
  const laArray = Object.values(cityData.localAuthorities);
  
  // Sort by number of schools (largest first)
  laArray.sort((a, b) => b.schools.length - a.schools.length);
  
  const html = laArray.map(la => {
    // Create URL-friendly slug for the LA
    const laSlug = la.name.toLowerCase().replace(/\s+/g, '-');
    const citySlug = cityData.name.toLowerCase().replace(/\s+/g, '-');
    
    // For non-England LAs, don't show Ofsted percentages
    let qualityIndicator = '';
    if (!cityData.isNonEngland) {
      const goodOrOutstanding = la.outstanding + la.good;
      const totalRated = la.schools.filter(s => s.ofsted_rating).length;
      const percentage = totalRated > 0 ? Math.round((goodOrOutstanding / totalRated) * 100) : 0;
      qualityIndicator = `
        <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #e5e7eb;">
          <div style="font-size: 0.875rem; color: #10b981; font-weight: 600;">
            ${percentage}% Good or Outstanding
          </div>
        </div>
      `;
    } else {
      // For non-England LAs, show average rating if available
      const ratings = la.schools
        .filter(s => s.overall_rating)
        .map(s => parseFloat(s.overall_rating));
      
      if (ratings.length > 0) {
        const avgRating = (ratings.reduce((a, b) => a + b, 0) / ratings.length).toFixed(1);
        qualityIndicator = `
          <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #e5e7eb;">
            <div style="font-size: 0.875rem; color: #2563eb; font-weight: 600;">
              Avg Rating: ${avgRating}/10
            </div>
          </div>
        `;
      }
    }
    
    return `
      <div class="la-card" onclick="window.location.href='/${citySlug}/${laSlug}'">
        <div class="la-card-name">${la.name}</div>
        <div class="la-card-stats">
          <div class="la-stat">
            <span class="la-stat-number">${la.schools.length}</span>
            <span class="la-stat-label"> schools</span>
          </div>
          <div class="la-stat">
            <span class="la-stat-number">${formatNumber(la.students)}</span>
            <span class="la-stat-label"> students</span>
          </div>
          <div class="la-stat">
            <span class="la-stat-number">${la.primary}</span>
            <span class="la-stat-label"> primary</span>
          </div>
          <div class="la-stat">
            <span class="la-stat-number">${la.secondary}</span>
            <span class="la-stat-label"> secondary</span>
          </div>
        </div>
        ${qualityIndicator}
      </div>
    `;
  }).join('');
  
  laGrid.innerHTML = html;
}

// Render Ofsted distribution (England only)
function renderOfstedDistribution() {
  if (cityData.isNonEngland) return; // Skip for non-England cities
  
  const totalInspected = Object.values(cityData.ofstedCounts).reduce((a, b) => a + b, 0) - 
                         (cityData.ofstedCounts.notInspected || 0);
  
  if (totalInspected > 0) {
    const outstandingPct = ((cityData.ofstedCounts.outstanding / totalInspected) * 100).toFixed(1);
    const goodPct = ((cityData.ofstedCounts.good / totalInspected) * 100).toFixed(1);
    const requiresPct = ((cityData.ofstedCounts.requiresImprovement / totalInspected) * 100).toFixed(1);
    const inadequatePct = ((cityData.ofstedCounts.inadequate / totalInspected) * 100).toFixed(1);
    
    // Update bars
    document.getElementById('outstandingBar').style.width = outstandingPct + '%';
    document.getElementById('outstandingPercent').textContent = outstandingPct + '%';
    
    document.getElementById('goodBar').style.width = goodPct + '%';
    document.getElementById('goodPercent').textContent = goodPct + '%';
    
    document.getElementById('requiresBar').style.width = requiresPct + '%';
    document.getElementById('requiresPercent').textContent = requiresPct + '%';
    
    document.getElementById('inadequateBar').style.width = inadequatePct + '%';
    document.getElementById('inadequatePercent').textContent = inadequatePct + '%';
    
    // Update summary
    const aboveAverage = parseFloat(outstandingPct) + parseFloat(goodPct);
    document.getElementById('aboveAveragePercent').textContent = aboveAverage.toFixed(1) + '%';
  }
}

// Fair ranking system that considers data completeness
function categorizeAndRankSchools(schools) {
  const tiers = {
    complete: [],
    partial: [],
    ofstedOnly: [],
    unrated: []
  };
  
  schools.forEach(school => {
    if (school.overall_rating !== null && school.overall_rating !== undefined) {
      // Determine completeness based on rating components or value
      const rating = parseFloat(school.overall_rating);
      
      // Check if school has comprehensive data (heuristic: non-standard Ofsted-only values)
      // Schools with only Ofsted would have ratings like 9, 7, 5, 3
      const isLikelyOfstedOnly = [9, 7, 5, 3].includes(Math.round(rating));
      
      if (school.rating_data_completeness >= 100) {
        tiers.complete.push(school);
      } else if (school.rating_data_completeness >= 40) {
        tiers.partial.push(school);
      } else if (isLikelyOfstedOnly && !school.rating_data_completeness) {
        tiers.ofstedOnly.push(school);
      } else {
        // Assume complete if we have a non-standard rating value
        tiers.complete.push(school);
      }
    } else if (school.ofsted_rating && !cityData.isScottish) {
      tiers.ofstedOnly.push(school);
    } else {
      tiers.unrated.push(school);
    }
  });
  
  // Sort each tier
  const sortByRating = (a, b) => {
    const ratingA = parseFloat(a.overall_rating) || 0;
    const ratingB = parseFloat(b.overall_rating) || 0;
    return ratingB - ratingA;
  };
  
  const sortByOfsted = (a, b) => {
    return (a.ofsted_rating || 5) - (b.ofsted_rating || 5);
  };
  
  tiers.complete.sort(sortByRating);
  tiers.partial.sort(sortByRating);
  tiers.ofstedOnly.sort(sortByOfsted);
  tiers.unrated.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
  
  return [...tiers.complete, ...tiers.partial, ...tiers.ofstedOnly, ...tiers.unrated];
}

// Render top schools with fair ranking
async function renderTopSchools() {
  // Render top 5 of each phase, syncing ratings with school API for consistency
  await renderSchoolList('primarySchools', cityData.schoolsByPhase.primary, 5);
  await renderSchoolList('secondarySchools', cityData.schoolsByPhase.secondary, 5);
  
  // Only render sixth form for non-Scottish cities
  if (!cityData.isScottish) {
    await renderSchoolList('sixthFormSchools', cityData.schoolsByPhase.sixthForm, 5);
  }
}

async function renderSchoolList(containerId, schools, showMax = 5) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  if (schools.length === 0) {
    container.innerHTML = '<div class="loading">No schools found in this category</div>';
    return;
  }
  
  // Apply fair ranking
  const rankedSchools = categorizeAndRankSchools(schools);
  let topSchools = rankedSchools.slice(0, showMax);

  // Fetch authoritative ratings for the displayed schools to match school page
  try {
    const details = await Promise.all(topSchools.map(s => 
      fetch(`/api/schools/${s.urn}`).then(r => r.ok ? r.json() : null).catch(() => null)
    ));
    topSchools = topSchools.map((s, i) => {
      const d = details[i];
      const school = d && d.school ? d.school : null;
      if (school && school.overall_rating != null) {
        return {
          ...s,
          overall_rating: school.overall_rating,
          rating_data_completeness: school.rating_data_completeness,
        };
      }
      return s;
    });
  } catch (e) {
    // If any fetch fails, fall back to existing data silently
    console.warn('Failed to sync ratings for list', e);
  }
  
  const html = topSchools.map((school, index) => {
    let ratingText = '—';
    let dataIndicator = '';
    let ratingValue = null;

    if (school.overall_rating != null) {
      const r = Number(school.overall_rating);
      
      // Cap rating at 10
      const cappedRating = Math.min(r, 10);
      
      // Check data completeness
      if (school.rating_data_completeness && school.rating_data_completeness < 40) {
        ratingText = '—';
        dataIndicator = ' <span style="color:#dc2626;font-size:0.7rem;" title="Insufficient data">⚠</span>';
      } else {
        // Show clean display for 10, one decimal otherwise
        ratingText = cappedRating === 10 ? '10' : cappedRating.toFixed(1);
        
        if (school.rating_data_completeness >= 100) {
          dataIndicator = ' <span style="color:#10b981;font-size:0.7rem;" title="Complete data">✓</span>';
        } else if (school.rating_data_completeness >= 40) {
          dataIndicator = ' <span style="color:#f59e0b;font-size:0.7rem;" title="Partial data">◐</span>';
        }
      }
      
      ratingValue = cappedRating;
    } else if (school.ofsted_rating != null && !cityData.isNonEngland) {
      // Fallback to Ofsted-based rating
      const fallback = ({1:9, 2:7, 3:5, 4:3})[school.ofsted_rating];
      if (fallback != null) {
        ratingText = fallback.toFixed(1);
        dataIndicator = ' <span style="color:#6b7280;font-size:0.7rem;" title="Ofsted only">※</span>';
        ratingValue = fallback;
      }
    }
    
    // Don't show Ofsted badge for non-England
    const ofstedBadge = (!cityData.isNonEngland && school.ofsted_rating) ? `
      <div class="ofsted-badge ${getOfstedClass(school.ofsted_rating)}">
        ${getOfstedLabel(school.ofsted_rating)}
      </div>` : '';
    
    return `
      <div class="school-item" onclick="window.location.href='${window.schoolPath ? window.schoolPath(school) : '/school/' + school.urn}'">
        <div class="school-rank">${index + 1}</div>
        <div class="school-details">
          <div class="school-name">${school.name}</div>
          <div class="school-info">
            <span class="school-info-item">📍 ${school.postcode || 'N/A'}</span>
            <span class="school-info-item">• ${school.type_of_establishment || 'School'}</span>
            ${school.number_on_roll ? `<span class="school-info-item">• ${formatNumber(school.number_on_roll)} students</span>` : ''}
          </div>
        </div>
        <div class="school-metrics">
          <div class="metric">
            <div class="metric-value">${ratingText}/10${dataIndicator}</div>
            <div class="metric-label">Rating</div>
          </div>
          ${ofstedBadge}
        </div>
      </div>
    `;
  }).join('');
  
  container.innerHTML = html;
}

// Switch between school phases
function switchPhase(phase) {
  // Don't allow switching to sixth form for non-England cities
  if (phase === 'sixth-form' && cityData.isNonEngland) {
    return;
  }
  
  document.querySelectorAll('.school-tab').forEach(tab => {
    tab.classList.remove('active');
    if (tab.dataset.phase === phase) {
      tab.classList.add('active');
    }
  });
  
  document.querySelectorAll('.school-list').forEach(list => {
    list.classList.remove('active');
  });
  
  if (phase === 'primary') {
    document.getElementById('primarySchools').classList.add('active');
  } else if (phase === 'secondary') {
    document.getElementById('secondarySchools').classList.add('active');
  } else if (phase === 'sixth-form') {
    document.getElementById('sixthFormSchools').classList.add('active');
  }
}

// Helper functions
function formatNumber(num) {
  if (!num) return '0';
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function getOfstedLabel(rating) {
  const labels = {
    1: 'Outstanding',
    2: 'Good',
    3: 'Requires Improvement',
    4: 'Inadequate'
  };
  return labels[rating] || 'Not Inspected';
}

function getOfstedClass(rating) {
  const classes = {
    1: 'outstanding',
    2: 'good',
    3: 'requires-improvement',
    4: 'inadequate'
  };
  return classes[rating] || '';
}

// Make switchPhase available globally
window.switchPhase = switchPhase;
