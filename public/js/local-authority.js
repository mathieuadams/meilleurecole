// local-authority.js - Complete JavaScript for Local Authority Page with fair ranking and Scotland support

// Global variables
let currentLA = null;
let schoolsByPhase = {
  primary: [],
  secondary: [],
  sixthForm: []
};
let isScottishLA = false; // legacy name; true for any non-England LA

function canonicalUrl() {
  const trimmedPath = window.location.pathname.endsWith('/') && window.location.pathname !== '/'
    ? window.location.pathname.slice(0, -1)
    : window.location.pathname;
  return `https://www.findschool.uk${trimmedPath || '/'}`;
}

function updateLAMeta(data, citySlug) {
  const laName = data.laName || 'Local Authority';
  const region = data.region ? data.region : '';
  const city = data.city || (citySlug ? citySlug.split('-').map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ') : '');
  const pageTitle = `${laName} Schools Overview | FindSchool.uk`;
  const url = canonicalUrl();
  const descriptionParts = [
    `Compare Ofsted ratings, performance data and student outcomes across the ${laName} local authority on FindSchool.uk.`
  ];
  if (region) descriptionParts.push(`Located in ${region}.`);
  if (city) descriptionParts.push(`Including schools in ${city} and surrounding communities.`);
  const description = descriptionParts.join(' ');

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
    const breadcrumbElements = [
      {
        "@type": "ListItem",
        "position": 1,
        "name": "Home",
        "item": "https://www.findschool.uk/"
      }
    ];
    if (city) {
      breadcrumbElements.push({
        "@type": "ListItem",
        "position": breadcrumbElements.length + 1,
        "name": city,
        "item": `https://www.findschool.uk/${(citySlug || city.toLowerCase().replace(/\s+/g, '-'))}`
      });
    }
    breadcrumbElements.push({
      "@type": "ListItem",
      "position": breadcrumbElements.length + 1,
      "name": laName,
      "item": url
    });

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
          "itemListElement": breadcrumbElements
        }
      ]
    };
    structuredDataEl.textContent = JSON.stringify(structuredData, null, 2);
  }
}

// Robust NI/UK school classifier using multiple fields
function classifySchoolNIAware(school) {
  const phase = (school.phase_of_education || '').toLowerCase();
  const type = (school.type_of_establishment || '').toLowerCase();
  const group = (school.establishment_group || '').toLowerCase();

  const isSpecial = type.includes('special') || phase.includes('special') || group.includes('special');
  if (isSpecial) return 'special';

  // All-through
  if (phase.includes('all-through') || phase.includes('through') || type.includes('all-through')) {
    return 'all-through';
  }

  const isPrimary = phase.includes('primary') || phase.includes('infant') || phase.includes('junior') || phase.includes('first') || type.includes('primary');
  if (isPrimary) return 'primary';

  const isSecondary = phase.includes('secondary') || phase.includes('middle') || phase.includes('high') || phase.includes('upper') ||
    /post[-\s]?primary/.test(phase) || /post[-\s]?primary/.test(type) || type.includes('secondary') || type.includes('grammar') || type.includes('high school');
  if (isSecondary) return 'secondary';

  // Sixth form detection (England/Northern Ireland)
  if (phase.includes('sixth') || phase.includes('16') || phase.includes('post-16') || type.includes('sixth') || type.includes('post-16')) {
    return 'sixth';
  }

  return null;
}

// Initialize page
(async function init() {
  // Extract LA and city from URL
  const path = window.location.pathname.split('/').filter(Boolean);
  let laSlug = null;
  let citySlug = null;
  
  if (path.length === 2) {
    // Format: /city/local-authority or /local-authority/name
    if (path[0] === 'local-authority') {
      laSlug = path[1];
    } else {
      citySlug = path[0];
      laSlug = path[1];
    }
  } else if (path.length === 1) {
    // Format: /local-authority-name (single segment)
    laSlug = path[0];
  }
  
  if (!laSlug) {
    console.error('No local authority specified in URL');
    document.getElementById('laName').textContent = 'Local Authority Not Found';
    return;
  }
  
  // Convert slug to LA name
  const laName = laSlug.split('-').map(word => 
    word.charAt(0).toUpperCase() + word.slice(1)
  ).join(' ');
  
  // Update LA breadcrumb
  const laCrumb = document.getElementById('laCrumb');
  if (laCrumb) laCrumb.textContent = laName;
  
  // Hide the trailing "School Name" breadcrumb on LA pages
  const schoolCrumb = document.getElementById('schoolCrumb');
  if (schoolCrumb) {
    const sep = schoolCrumb.previousElementSibling;
    schoolCrumb.style.display = 'none';
    if (sep && sep.classList.contains('breadcrumb-separator')) sep.style.display = 'none';
  }
  
  // Hide city breadcrumb if we're on a direct LA page
  if (!citySlug) {
    const cityCrumb = document.getElementById('cityCrumb');
    if (cityCrumb) {
      cityCrumb.style.display = 'none';
      const citySeparator = cityCrumb.previousElementSibling;
      if (citySeparator && citySeparator.classList.contains('breadcrumb-separator')) {
        citySeparator.style.display = 'none';
      }
    }
  }
  
  // Load LA data
  await loadLocalAuthorityData(laName, citySlug);
})();

// Check LA country using first school's country; treat non-England as 'Scottish' flag for UI logic
async function checkIfScottishLA(schools) {
  if (schools && schools.length > 0) {
    try {
      const firstSchool = schools[0];
      if (firstSchool && firstSchool.urn) {
        const response = await fetch(`/api/schools/${firstSchool.urn}`);
        const data = await response.json();
        const country = data.school?.country || 'England';
        return country.toLowerCase() !== 'england';
      }
    } catch (error) {
      console.error('Error checking country:', error);
    }
  }
  return false;
}

// Hide England-specific features for non-England LAs
function hideScotlandFeatures() {
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
  
  // Hide Science performance card if present
  const performanceGrid = document.querySelector('.performance-grid');
  if (performanceGrid) {
    const cards = performanceGrid.querySelectorAll('.performance-card');
    cards.forEach(card => {
      const label = card.querySelector('.performance-label');
      if (label && label.textContent.toLowerCase().includes('science')) {
        card.style.display = 'none';
      }
    });
  }
}

// Load local authority data
async function loadLocalAuthorityData(laName, citySlug) {
  try {
    // Fetch LA summary data
    const response = await fetch(`/api/local-authority/${encodeURIComponent(laName)}/summary`);
    
    if (!response.ok) {
      // If API endpoint doesn't exist yet, use search API as fallback
      await loadUsingSearchAPI(laName, citySlug);
      return;
    }
    
    const data = await response.json();
    
    if (data.success) {
      // Check if this is a Scottish LA
      isScottishLA = await checkIfScottishLA(data.schools);
      
      // Hide England-specific features if Scottish
      if (isScottishLA) {
        hideScotlandFeatures();
      }
      
      renderLASummary(data, citySlug);
      processSchoolsForPhases(data.schools);
      await renderTopSchools();
    }
  } catch (error) {
    console.error('Error loading LA data:', error);
    // Fallback to search API
    await loadUsingSearchAPI(laName, citySlug);
  }
}

// Fallback: Load using search API
async function loadUsingSearchAPI(laName, citySlug) {
  try {
    const response = await fetch(`/api/search?type=location&q=${encodeURIComponent(laName)}&limit=500`);
    const data = await response.json();
    
    if (data.success && data.schools) {
      const schools = data.schools;
      
      // Check if this is a Scottish LA
      isScottishLA = await checkIfScottishLA(schools);
      
      // Hide England-specific features if Scottish
      if (isScottishLA) {
        hideScotlandFeatures();
      }
      
      // Process and categorize schools
      const summary = processSchoolsData(schools, laName);
      
      // Try to extract city from school data if we don't have it
      if (!citySlug && schools.length > 0) {
        const towns = schools.map(s => s.town).filter(Boolean);
        if (towns.length > 0) {
          const townCounts = {};
          towns.forEach(town => {
            townCounts[town] = (townCounts[town] || 0) + 1;
          });
          const mostCommonTown = Object.keys(townCounts).reduce((a, b) => 
            townCounts[a] > townCounts[b] ? a : b
          );
          summary.city = mostCommonTown;
        }
      }
      
      renderLASummary(summary, citySlug);
      processSchoolsForPhases(schools);
      await renderTopSchools();
    }
  } catch (error) {
    console.error('Error loading schools via search:', error);
    document.getElementById('laName').textContent = 'Error Loading Local Authority';
  }
}

// Process schools data to generate summary
function processSchoolsData(schools, laName) {
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
  
  let englishScores = [];
  let mathScores = [];
  let scienceScores = [];
  let attendanceRates = [];
  let fsmPercentages = [];
  
  schools.forEach(school => {
    const cls = classifySchoolNIAware(school);
    
    // Check for special schools FIRST
    if (cls === 'special') {
      specialCount++;
    } 
    // Then categorize by phase (non-special schools only)
    else if (cls === 'primary' || cls === 'all-through') {
      primaryCount++;
    }
    else if (cls === 'secondary' || cls === 'all-through') {
      secondaryCount++;
      
      // Check if this secondary school also has sixth form (not relevant for Scotland)
      if (!isScottishLA) {
        sixthFormCount++;
      }
    }
    else if (phase.includes('all-through') || phase.includes('through')) {
      // All-through schools count as both
      primaryCount++;
      secondaryCount++;
    }
    
    // Count students: prefer number_on_roll, fallback to total_pupils (common in NI)
    const pupils = school.number_on_roll ?? school.total_pupils;
    if (pupils) totalStudents += parseInt(pupils) || 0;
    
    // Count Ofsted ratings (only for non-Scottish schools)
    if (!isScottishLA) {
      switch(school.ofsted_rating) {
        case 1: ofstedCounts.outstanding++; break;
        case 2: ofstedCounts.good++; break;
        case 3: ofstedCounts.requiresImprovement++; break;
        case 4: ofstedCounts.inadequate++; break;
        default: ofstedCounts.notInspected++; break;
      }
    }
    
    // Collect performance metrics
    if (school.english_score) englishScores.push(parseFloat(school.english_score));
    if (school.math_score) mathScores.push(parseFloat(school.math_score));
    
    // Don't collect science scores for Scottish schools
    if (!isScottishLA && school.science_score) {
      scienceScores.push(parseFloat(school.science_score));
    }
    
    if (school.overall_absence_rate) {
      attendanceRates.push(100 - parseFloat(school.overall_absence_rate));
    }
    if (school.fsm_percentage) fsmPercentages.push(parseFloat(school.fsm_percentage));
  });
  
  // Calculate averages
  const avg = arr => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(1) : null;
  
  return {
    laName: laName,
    totalSchools: schools.length,
    totalStudents: totalStudents,
    primaryCount: primaryCount,
    secondaryCount: secondaryCount,
    sixthFormCount: sixthFormCount,
    specialCount: specialCount,
    ofstedCounts: ofstedCounts,
    avgEnglish: avg(englishScores),
    avgMaths: avg(mathScores),
    avgScience: isScottishLA ? null : avg(scienceScores),
    avgAttendance: avg(attendanceRates),
    avgFSM: avg(fsmPercentages),
    schools: schools
  };
}

// Process schools into phases
function processSchoolsForPhases(schools) {
  // Clear phase arrays
  schoolsByPhase.primary = [];
  schoolsByPhase.secondary = [];
  schoolsByPhase.sixthForm = [];
  
  schools.forEach(school => {
    const cls = classifySchoolNIAware(school);
    if (cls === 'primary' || cls === 'all-through') schoolsByPhase.primary.push(school);
    if (cls === 'secondary' || cls === 'all-through') schoolsByPhase.secondary.push(school);
    if (!isScottishLA && (cls === 'sixth' || cls === 'secondary')) schoolsByPhase.sixthForm.push(school);
  });
}

// Render LA summary
function renderLASummary(data, citySlug) {
  // Update header
  document.getElementById('laName').textContent = data.laName;
  document.getElementById('laSchoolCount').textContent = data.totalSchools || '0';
  document.getElementById('laStudentCount').textContent = formatNumber(data.totalStudents) || '0';
  document.getElementById('laLocation').textContent = data.region || data.laName;
  
  // Update city breadcrumb if we have city information
  const cityCrumb = document.getElementById('cityCrumb');
  if (cityCrumb) {
    if (data.city || citySlug) {
      const city = data.city || (citySlug ? citySlug.split('-').map(w => 
        w.charAt(0).toUpperCase() + w.slice(1)).join(' ') : null);
      
      if (city) {
        cityCrumb.textContent = city;
        // For Wales and Northern Ireland, link to search page to avoid 404 for unknown city routes
        const regionLower = (data.region || '').toLowerCase();
        const isNonEngland = regionLower && regionLower !== 'england';
        if (isNonEngland) {
          cityCrumb.href = `/search?type=location&q=${encodeURIComponent(city)}`;
        } else {
          cityCrumb.href = `/${citySlug || city.toLowerCase().replace(/\s+/g, '-')}`;
        }
        cityCrumb.style.display = 'inline';
      } else {
        cityCrumb.style.display = 'none';
        const citySeparator = cityCrumb.previousElementSibling;
        if (citySeparator && citySeparator.classList.contains('breadcrumb-separator')) {
          citySeparator.style.display = 'none';
        }
      }
    } else {
      cityCrumb.style.display = 'none';
      const citySeparator = cityCrumb.previousElementSibling;
      if (citySeparator && citySeparator.classList.contains('breadcrumb-separator')) {
        citySeparator.style.display = 'none';
      }
    }
  }

  // Ensure LA breadcrumb points to canonical route (avoids reliance on city slug)
  const laCrumb = document.getElementById('laCrumb');
  if (laCrumb && data.laName) {
    const laSlug = data.laName.toLowerCase().replace(/\s+/g, '-');
    laCrumb.href = `/local-authority/${laSlug}`;
  }
  
  // Update meta and title
  updateLAMeta(data, citySlug);
  document.getElementById('laNameFooter').textContent = data.laName;
  
  // Update counts
  document.getElementById('primaryCount').textContent = data.primaryCount || '0';
  document.getElementById('secondaryCount').textContent = data.secondaryCount || '0';
  
  // Only show sixth form count for non-Scottish LAs
  if (!isScottishLA) {
    document.getElementById('sixthFormCount').textContent = data.sixthFormCount || '0';
  }
  
  document.getElementById('specialCount').textContent = data.specialCount || '0';
  
  // Calculate and render Ofsted distribution (only for non-Scottish LAs)
  if (!isScottishLA) {
    const totalInspected = Object.values(data.ofstedCounts).reduce((a, b) => a + b, 0) - (data.ofstedCounts.notInspected || 0);
    
    if (totalInspected > 0) {
      const outstandingPct = ((data.ofstedCounts.outstanding / totalInspected) * 100).toFixed(1);
      const goodPct = ((data.ofstedCounts.good / totalInspected) * 100).toFixed(1);
      const requiresPct = ((data.ofstedCounts.requiresImprovement / totalInspected) * 100).toFixed(1);
      const inadequatePct = ((data.ofstedCounts.inadequate / totalInspected) * 100).toFixed(1);
      
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
  
  // Update performance metrics
  if (data.avgEnglish) {
    document.getElementById('avgEnglish').textContent = data.avgEnglish + '%';
    document.getElementById('englishComparison').textContent = 'LA Average';
  }
  if (data.avgMaths) {
    document.getElementById('avgMaths').textContent = data.avgMaths + '%';
    document.getElementById('mathsComparison').textContent = 'LA Average';
  }
  
  // Only show Science for non-Scottish LAs
  if (!isScottishLA && data.avgScience) {
    // Science elements should already be hidden by hideScotlandFeatures()
  }
  
  if (data.avgAttendance) {
    document.getElementById('avgAttendance').textContent = data.avgAttendance + '%';
    document.getElementById('attendanceComparison').textContent = 'LA Average';
  }
  if (data.avgFSM) {
    document.getElementById('avgFSM').textContent = data.avgFSM + '%';
  }
  
  // Update view all link
  const viewAllLink = document.getElementById('viewAllLink');
  if (viewAllLink) {
    viewAllLink.href = `/search?type=location&q=${encodeURIComponent(data.laName)}`;
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
      // Determine completeness
      const rating = parseFloat(school.overall_rating);
      
      // Check if school has comprehensive data
      const isLikelyOfstedOnly = [9, 7, 5, 3].includes(Math.round(rating));
      
      // Adjust thresholds for Scottish schools
      const completeThreshold = isScottishLA ? 100 : 100;
      const partialThreshold = isScottishLA ? 50 : 40;
      
      if (school.rating_data_completeness >= completeThreshold) {
        tiers.complete.push(school);
      } else if (school.rating_data_completeness >= partialThreshold) {
        tiers.partial.push(school);
      } else if (isLikelyOfstedOnly && !school.rating_data_completeness && !isScottishLA) {
        tiers.ofstedOnly.push(school);
      } else {
        // Assume complete if we have a non-standard rating value
        tiers.complete.push(school);
      }
    } else if (school.ofsted_rating && !isScottishLA) {
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
  await renderSchoolList('primarySchools', schoolsByPhase.primary, 5);
  await renderSchoolList('secondarySchools', schoolsByPhase.secondary, 5);
  
  // Only render sixth form for non-Scottish LAs
  if (!isScottishLA) {
    await renderSchoolList('sixthFormSchools', schoolsByPhase.sixthForm, 5);
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

  // Sync ratings with the school API for the rendered items
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
    console.warn('Failed to sync ratings for LA list', e);
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
    } else if (school.ofsted_rating != null && !isScottishLA) {
      // Fallback to Ofsted-based rating
      const fallback = ({1:9, 2:7, 3:5, 4:3})[school.ofsted_rating];
      if (fallback != null) {
        ratingText = fallback.toFixed(1);
        dataIndicator = ' <span style="color:#6b7280;font-size:0.7rem;" title="Ofsted only">※</span>';
        ratingValue = fallback;
      }
    }
    
    // Don't show Ofsted badge for Scottish schools
    const ofstedBadge = (!isScottishLA && school.ofsted_rating) ? `
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
  // Don't allow switching to sixth form for Scottish LAs
  if (phase === 'sixth-form' && isScottishLA) {
    return;
  }
  
  // Update tabs
  document.querySelectorAll('.school-tab').forEach(tab => {
    tab.classList.remove('active');
    if (tab.dataset.phase === phase) {
      tab.classList.add('active');
    }
  });
  
  // Update content
  document.querySelectorAll('.school-list').forEach(list => {
    list.classList.remove('active');
  });
  
  if (phase === 'primary') {
    document.getElementById('primarySchools').classList.add('active');
  } else if (phase === 'secondary') {
    document.getElementById('secondarySchools').classList.add('active');
  } else if (phase === 'sixth-form' && !isScottishLA) {
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
