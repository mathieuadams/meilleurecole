// Main JavaScript pour MeilleureEcole.fr

// API Base URL - automatically uses the same domain
const API_BASE_URL = '/api';

let mobileMenuButton = null;
let mobileMenuContainer = null;

function normalizePath(path) {
    if (!path) return '/';
    const cleaned = path.split('?')[0].split('#')[0].replace(/\.html$/i, '');
    if (cleaned === '' || cleaned === '/') {
        return '/';
    }
    const trimmed = cleaned.replace(/\/$/, '');
    if (trimmed === '') {
        return '/';
    }
    return trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
}

function pathsMatch(linkPath, currentPath) {
    const normalizedLink = normalizePath(linkPath);
    const normalizedCurrent = normalizePath(currentPath);

    if (normalizedLink === normalizedCurrent) return true;

    if (normalizedLink === '/review' && (normalizedCurrent === '/write-review')) return true;
    if (normalizedLink === '/' && (normalizedCurrent === '/index')) return true;

    return false;
}

function highlightActiveNavLinks() {
    const currentPath = window.location.pathname;
    const allNavLinks = document.querySelectorAll('.nav-link, .mobile-nav-link');
    if (!allNavLinks.length) return;

    allNavLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (!href) {
            link.classList.remove('active');
            link.removeAttribute('aria-current');
            return;
        }

        const isActive = pathsMatch(href, currentPath);
        link.classList.toggle('active', isActive);
        if (isActive) {
            link.setAttribute('aria-current', 'page');
        } else {
            link.removeAttribute('aria-current');
        }
    });
}

function setMobileMenuState(shouldOpen) {
    if (!mobileMenuButton || !mobileMenuContainer) return;

    mobileMenuButton.classList.toggle('active', shouldOpen);
    mobileMenuButton.setAttribute('aria-expanded', shouldOpen ? 'true' : 'false');

    mobileMenuContainer.classList.toggle('active', shouldOpen);
    mobileMenuContainer.setAttribute('aria-hidden', shouldOpen ? 'false' : 'true');

    document.body.classList.toggle('mobile-menu-open', shouldOpen);
}

function toggleMobileMenu(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }

    const shouldOpen = !mobileMenuButton?.classList.contains('active');
    setMobileMenuState(shouldOpen);
}

function closeMobileMenu() {
    if (!mobileMenuButton?.classList.contains('active')) return;
    setMobileMenuState(false);
}

function handleDocumentClick(event) {
    if (!mobileMenuContainer || !mobileMenuButton) return;
    if (mobileMenuContainer.contains(event.target) || mobileMenuButton.contains(event.target)) {
        return;
    }
    closeMobileMenu();
}

function initializeHeader() {
    const headerEl = document.querySelector('.navbar');
    const menuBtn = document.querySelector('.mobile-menu-btn');
    const menuPanel = document.getElementById('mobileMenu');

    if (!headerEl || !menuBtn || !menuPanel) {
        return;
    }

    mobileMenuButton = menuBtn;
    mobileMenuContainer = menuPanel;

    highlightActiveNavLinks();
    setMobileMenuState(false);

    if (menuBtn.dataset.bound !== 'true') {
        menuBtn.addEventListener('click', toggleMobileMenu);
        menuPanel.querySelectorAll('a').forEach(link => {
            link.addEventListener('click', closeMobileMenu);
        });
        menuBtn.dataset.bound = 'true';
    }

    if (document.body && !document.body.dataset.mobileMenuListener) {
        document.addEventListener('click', handleDocumentClick);
        document.body.dataset.mobileMenuListener = 'true';
    }
}

window.initializeHeader = initializeHeader;

document.addEventListener('componentsLoaded', initializeHeader);
window.addEventListener('popstate', () => {
    highlightActiveNavLinks();
    closeMobileMenu();
});

// Ensure favicon is available on every page regardless of template differences
function ensureFavicon() {
    const head = document.head;
    if (!head) return;

    const iconHref = '/favicon.ico?v=2';

    let iconLink = head.querySelector('link[rel="icon"]');
    if (!iconLink) {
        iconLink = document.createElement('link');
        iconLink.rel = 'icon';
        head.appendChild(iconLink);
    }
    iconLink.href = iconHref;
    iconLink.type = 'image/x-icon';
    iconLink.sizes = 'any';

    let shortcutLink = head.querySelector('link[rel="shortcut icon"]');
    if (!shortcutLink) {
        shortcutLink = document.createElement('link');
        shortcutLink.rel = 'shortcut icon';
        head.appendChild(shortcutLink);
    }
    shortcutLink.href = iconHref;
    shortcutLink.type = 'image/x-icon';
}

// Utility function to format numbers
function formatNumber(num) {
    if (!num) return '-';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function schoolSlug(name = '') {
    if (!name) return '';
    return name
        .toLowerCase()
        .replace(/&/g, ' and ')
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}

function schoolPathFromData(schoolLike) {
    if (!schoolLike) return '/school';
    const urn = schoolLike.urn || schoolLike.id || '';
    const name = schoolLike.name || schoolLike.school_name || '';
    if (!urn) return '/school';
    const slug = schoolSlug(name);
    return slug ? `/school/${urn}-${slug}` : `/school/${urn}`;
}

window.schoolSlug = schoolSlug;
window.schoolPath = schoolPathFromData;
window.schoolPathFromData = schoolPathFromData;

// Map tiles helper to comply with OSM tile usage policy
// Prefer a commercial/free key-based provider if a public key is available.
// Fallback to CARTO basemaps (which allow anonymous usage with attribution) rather than OSM tiles.
window.getDefaultTileLayer = function(map) {
  try {
    const key = window.MAPTILER_KEY || window.maptilerKey || null;
    if (key) {
      const url = `https://api.maptiler.com/maps/streets-v2/256/{z}/{x}/{y}.png?key=${key}`;
      return L.tileLayer(url, {
        attribution: '© OpenStreetMap contributors, © MapTiler',
        maxZoom: 20
      }).addTo(map);
    }
  } catch {}
  // CARTO Positron
  const cartoUrl = 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';
  return L.tileLayer(cartoUrl, {
    attribution: '© OpenStreetMap contributors, © CARTO',
    subdomains: 'abcd',
    maxZoom: 19
  }).addTo(map);
};

// Utility function to get URL parameters
function getUrlParameter(name) {
    name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
    const regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
    const results = regex.exec(location.search);
    return results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
}

// Load header and footer components
async function loadComponents() {
    try {
        // Load header
        const headerResponse = await fetch('/components/header.html');
        if (headerResponse.ok) {
            const headerHTML = await headerResponse.text();
            const headerElement = document.getElementById('header');
            if (headerElement) {
                headerElement.innerHTML = headerHTML;
                initializeHeader();
            }
        }
        
        // Load footer
        const footerResponse = await fetch('/components/footer.html');
        if (footerResponse.ok) {
            const footerHTML = await footerResponse.text();
            const footerElement = document.getElementById('footer');
            if (footerElement) {
                footerElement.innerHTML = footerHTML;
            }
        }
    } catch (error) {
        console.error('Error loading components:', error);
    }
}

// Search functionality
async function searchSchools(query, type = 'all') {
    try {
        const response = await fetch(`${API_BASE_URL}/search?q=${encodeURIComponent(query)}&type=${type}`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error searching schools:', error);
        return { error: 'Failed to search schools' };
    }
}

// Get school by URN
async function getSchool(urn) {
    try {
        const response = await fetch(`${API_BASE_URL}/schools/${urn}`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching school:', error);
        return { error: 'Failed to fetch school data' };
    }
}

// Get school performance data
async function getSchoolPerformance(urn) {
    try {
        const response = await fetch(`${API_BASE_URL}/schools/${urn}/performance`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching performance data:', error);
        return { error: 'Failed to fetch performance data' };
    }
}

// Get nearby schools
async function getNearbySchools(urn, limit = 5) {
    try {
        const response = await fetch(`${API_BASE_URL}/schools/${urn}/nearby?limit=${limit}`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching nearby schools:', error);
        return { error: 'Failed to fetch nearby schools' };
    }
}

// Display search results
function displaySearchResults(schools) {
    const container = document.getElementById('searchResults');
    if (!container) return;
    
    if (!schools || schools.length === 0) {
        container.innerHTML = '<p>No schools found. Try a different search term.</p>';
        return;
    }
    
    const html = schools.map(school => `
        <div class="school-card" onclick="window.location.href='${schoolPathFromData(school)}'">
            <div class="school-card-header">
                <div>
                    <div class="school-card-name">${school.name}</div>
                    <div class="school-card-type">${school.type_of_establishment} • ${school.phase_of_education}</div>
                </div>
                <div class="school-card-rating rating-${getRatingClass(school.overall_rating)}">
                    ${school.overall_rating}/10
                </div>
            </div>
            <div class="school-card-details">
                <div class="detail-item">
                    <span>📍</span>
                    <span>${school.postcode}</span>
                </div>
                <div class="detail-item">
                    <span>🎓</span>
                    <span>Ofsted: ${getOfstedLabel(school.ofsted_rating)}</span>
                </div>
                <div class="detail-item">
                    <span>👥</span>
                    <span>${formatNumber(school.number_on_roll)} students</span>
                </div>
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

// Get rating class for styling
function getRatingClass(rating) {
    if (!rating) return 'average';
    if (rating >= 8) return 'excellent';
    if (rating >= 6) return 'good';
    if (rating >= 4) return 'satisfactory';
    return 'poor';
}

// Get Ofsted label from rating number
function getOfstedLabel(rating) {
    const labels = {
        1: 'Outstanding',
        2: 'Good',
        3: 'Requires Improvement',
        4: 'Inadequate'
    };
    return labels[rating] || 'Not Inspected';
}

async function getSearchSuggestions(query) {
  if (!query || query.length < 2) return [];
  try {
    const res = await fetch(`/api/search/suggest?q=${encodeURIComponent(query)}`);
    const data = await res.json();

    const schools = (data.schools || []).map(s => ({
      type: 'school',
      id: s.urn,
      suggestion: s.name,
      town: s.town,
      postcode: s.postcode,
      overall_rating: s.overall_rating
    }));

    const cities = (data.cities || []).map(c => ({
      type: 'city',
      suggestion: c.town
    }));

    const las = (data.authorities || []).map(a => ({
      type: 'la',
      suggestion: a.local_authority
    }));

    const pcs = (data.postcodes || []).map(p => ({
      type: 'pc',
      suggestion: p.postcode
    }));

    return [...schools, ...cities, ...las, ...pcs];
  } catch (e) {
    console.error('Error fetching suggestions:', e);
    return [];
  }
}


// Display search suggestions
function displaySuggestions(suggestions, inputElement) {
    // Remove existing suggestions container
    const existingSuggestions = document.querySelector('.search-suggestions-dropdown');
    if (existingSuggestions) {
        existingSuggestions.remove();
    }
    
    if (!suggestions || suggestions.length === 0) return;
    
    // Create suggestions container
    const suggestionsDiv = document.createElement('div');
    suggestionsDiv.className = 'search-suggestions-dropdown';
    suggestionsDiv.style.cssText = `
        position: absolute;
        top: 100%;
        left: 0;
        right: 0;
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        margin-top: 4px;
        box-shadow: 0 10px 25px rgba(0,0,0,0.1);
        max-height: 300px;
        overflow-y: auto;
        z-index: 1000;
    `;
    
    // Add suggestions
    suggestions.forEach(suggestion => {
        const item = document.createElement('div');
        item.className = 'suggestion-item';
        item.style.cssText = `
            padding: 12px 16px;
            cursor: pointer;
            border-bottom: 1px solid #f3f4f6;
            display: flex;
            align-items: center;
            gap: 10px;
            transition: background 0.2s;
        `;
        
        // Add icon based on type
        const icon =
        suggestion.type === 'school' ? '🏫' :
        suggestion.type === 'city'   ? '📍' :
        suggestion.type === 'pc'     ? '📮' :
        suggestion.type === 'la'     ? '🏛️' : '📍';

        
        item.innerHTML = `
            <span style="font-size: 1.2em;">${icon}</span>
            <div style="flex: 1;">
                <div style="font-weight: 500; color: #111827;">${suggestion.suggestion}</div>
                <div style="font-size: 0.875rem; color: #6b7280; text-transform: capitalize;">${suggestion.type}</div>
            </div>
        `;

        if (suggestion.type === 'school' && suggestion.overall_rating != null) {
        const chip = document.createElement('div');
        chip.className = 'suggest-badge';
        chip.textContent = Number(suggestion.overall_rating).toFixed(1) + '/10';
        item.appendChild(chip);
        }

        
        // Add hover effect
        item.addEventListener('mouseenter', () => {
            item.style.background = '#f9fafb';
        });
        item.addEventListener('mouseleave', () => {
            item.style.background = 'white';
        });
        
        // Handle click
        item.addEventListener('click', () => {
            inputElement.value = suggestion.suggestion;
            suggestionsDiv.remove();
            
            // If it's a school, go directly to the school page
            if (suggestion.type === 'school' && suggestion.id) {
                window.location.href = schoolPathFromData({ urn: suggestion.id, name: suggestion.suggestion });
            } else {
                // Otherwise, perform a search
                const form = inputElement.closest('form');
                if (form) {
                    form.dispatchEvent(new Event('submit'));
                }
            }
        });
        
        suggestionsDiv.appendChild(item);
    });
    
    // Add suggestions container to the parent of the input
    const inputParent = inputElement.parentElement;
    inputParent.style.position = 'relative';
    inputParent.appendChild(suggestionsDiv);
}

// Initialize on DOM load
document.addEventListener('DOMContentLoaded', function() {
    ensureFavicon();

    // Load header and footer
    loadComponents();
    
    // Set up search input with autocomplete
    const searchInputs = document.querySelectorAll('#searchInput, #mainSearchInput');
    searchInputs.forEach(input => {
        if (input) {
            let debounceTimer;
            
            // Add input event for autocomplete
            input.addEventListener('input', async function(e) {
                const query = e.target.value;
                
                // Clear previous timer
                clearTimeout(debounceTimer);
                
                // Remove suggestions if query is too short
                if (query.length < 2) {
                    const existingSuggestions = document.querySelector('.search-suggestions-dropdown');
                    if (existingSuggestions) {
                        existingSuggestions.remove();
                    }
                    return;
                }
                
                // Debounce the API call
                debounceTimer = setTimeout(async () => {
                    const suggestions = await getSearchSuggestions(query);
                    displaySuggestions(suggestions, input);
                }, 300); // Wait 300ms after user stops typing
            });
            
            // Hide suggestions when clicking outside
            document.addEventListener('click', function(e) {
                if (!input.contains(e.target) && !e.target.closest('.search-suggestions-dropdown')) {
                    const existingSuggestions = document.querySelector('.search-suggestions-dropdown');
                    if (existingSuggestions) {
                        existingSuggestions.remove();
                    }
                }
            });
            
            // Hide suggestions on escape key
            input.addEventListener('keydown', function(e) {
                if (e.key === 'Escape') {
                    const existingSuggestions = document.querySelector('.search-suggestions-dropdown');
                    if (existingSuggestions) {
                        existingSuggestions.remove();
                    }
                }
            });
        }
    });
    
    // Set up search form if it exists
    const searchForm = document.getElementById('searchForm');
    if (searchForm) {
        searchForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            const query = document.getElementById('searchInput').value;
            const type = document.querySelector('.search-tab.active')?.dataset.type || 'all';
            
            // Hide suggestions
            const existingSuggestions = document.querySelector('.search-suggestions-dropdown');
            if (existingSuggestions) {
                existingSuggestions.remove();
            }
            
            // Show loading
            const resultsContainer = document.getElementById('searchResults');
            if (resultsContainer) {
                resultsContainer.innerHTML = '<p>Searching...</p>';
            }
            
            // Perform search
            const results = await searchSchools(query, type);
            
            // Display results
            if (results.schools) {
                displaySearchResults(results.schools);
            }
        });
    }
    
    // Handle search type tabs
    const searchTabs = document.querySelectorAll('.search-tab');
    searchTabs.forEach(tab => {
        tab.addEventListener('click', function() {
            searchTabs.forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            
            // Update search placeholder
            const searchInput = document.getElementById('searchInput');
            if (searchInput) {
                const type = this.dataset.type;
                switch(type) {
                    case 'postcode':
                        searchInput.placeholder = "Saisissez un code postal (ex. 75001)...";
                        break;
                    case 'name':
                        searchInput.placeholder = "Saisissez le nom d'un etablissement...";
                        break;
                    case 'location':
                        searchInput.placeholder = "Saisissez une ville ou une commune...";
                        break;
                    default:
                        searchInput.placeholder = "Saisissez un code postal, un etablissement ou une ville...";
                }
            }
        });
    });
    
    // Check if we're on a school page
    const path = window.location.pathname;
    if (path.startsWith('/school/')) {
        const urn = path.split('/').pop();
        if (urn) {
            loadSchoolData(urn);
        }
    }
});

// Load school data for school profile page
async function loadSchoolData(urn) {
    try {
        // Load basic school data
        const schoolData = await getSchool(urn);
        if (schoolData.success && schoolData.school) {
            updateSchoolProfile(schoolData.school);
        }
        
        // Load performance data
        const perfData = await getSchoolPerformance(urn);
        if (perfData.success && perfData.performance) {
            updatePerformanceData(perfData.performance);
        }
        
        // Load nearby schools
        const nearbyData = await getNearbySchools(urn);
        if (nearbyData.success && nearbyData.nearby_schools) {
            updateNearbySchools(nearbyData.nearby_schools);
        }
    } catch (error) {
        console.error('Erreur lors du chargement des donnees etablissement:', error);
    }
}

// Update school profile with data
function updateSchoolProfile(school) {
    // Update all the elements with school data
    const elements = {
        'schoolName': school.name,
        'schoolType': school.type,
        'schoolPhase': school.phase,
        'schoolAddress': `${school.address.street}, ${school.address.town}, ${school.address.postcode}`,
        'overallRating': school.overall_rating + '/10',
        'statStudents': formatNumber(school.demographics.total_students),
        'statFSM': school.demographics.fsm_percentage + '%',
        'infoURN': school.urn,
        'infoType': school.type,
        'infoGender': school.characteristics.gender,
        'infoAgeRange': school.characteristics.age_range,
        'infoLA': school.address.local_authority
    };
    
    for (const [id, value] of Object.entries(elements)) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value || '-';
        }
    }
    
    // Update Ofsted rating with styling
    const ofstedElement = document.getElementById('ofstedRating');
    if (ofstedElement && school.ofsted) {
        const ofstedLabel = getOfstedLabel(school.ofsted.overall_effectiveness);
        ofstedElement.textContent = ofstedLabel;
        ofstedElement.className = `ofsted-score ofsted-${ofstedLabel.toLowerCase().replace(/\s+/g, '-')}`;
    }
    
    // Update SEO meta if available
    if (typeof updateSchoolMeta === 'function') {
        updateSchoolMeta(school);
    } else {
        document.title = `${school.name} | Profil etablissement | MeilleureEcole.fr`;
    }
}

// Update performance data display
function updatePerformanceData(performance) {
    // This would update the performance grid based on the school's phase
    // Implementation depends on your specific HTML structure
    console.log('Donnees de performance:', performance);
}

// Update nearby schools list
function updateNearbySchools(schools) {
    const container = document.getElementById('nearbySchools');
    if (!container) return;
    
    const html = schools.slice(0, 5).map(school => `
        <div class="nearby-school" onclick="window.location.href='${schoolPathFromData(school)}'">
            <div>
                <div class="nearby-school-name">${school.name}</div>
                <div class="nearby-school-distance">${school.type_of_establishment}</div>
            </div>
            <div class="nearby-school-rating">${school.overall_rating}/10</div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

// Export functions for use in HTML
window.searchSchools = searchSchools;
window.getSchool = getSchool;
window.formatNumber = formatNumber;
window.getOfstedLabel = getOfstedLabel;
