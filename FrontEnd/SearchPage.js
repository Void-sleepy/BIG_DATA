let allDeals = [];

fetch('airfryer_deals_20250524_150235.json')
  .then(response => response.json())
  .then(data => {
    allDeals = data.map(deal => ({
      name: deal.product_name,
      description: `${deal.category.replace(/_/g, ' ')} • ${deal.discount_percentage}% off`,
      price: `${deal.price.toFixed(2)} ${deal.currency}`,
      site: deal.retailer,
      category: deal.category,
      link: deal.product_url
    }));

    populateFilters(allDeals);
    renderDeals(allDeals);
  })
  .catch(error => {
    console.error("Error loading deals:", error);
  });

function capitalizeWords(str) {
  return str
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function populateFilters(deals) {
  const retailerSelect = document.getElementById('retailerFilter');
  const categorySelect = document.getElementById('categoryFilter');

  const retailers = [...new Set(deals.map(d => d.site))];
  const categories = [...new Set(deals.map(d => d.category))];

  retailers.forEach(r => {
    const option = document.createElement('option');
    option.value = r;
    option.textContent = r;
    retailerSelect.appendChild(option);
  });

  categories.forEach(c => {
    const option = document.createElement('option');
    option.value = c;
    option.textContent = capitalizeWords(c);  // use the capitalized version
    categorySelect.appendChild(option);
  });
}


function renderDeals(dealList) {
  const grid = document.getElementById('dealGrid');
  grid.innerHTML = '';

  dealList.forEach(deal => {
    const card = document.createElement('a');
    card.href = deal.link;
    card.target = "_blank";
    card.className = 'deal-card';

    card.innerHTML = `
      <div class="deal-content">
        <h3>${deal.name}</h3>
        <p>${deal.description}</p>
        <p class="site-name">${deal.site}</p>
        <p class="price">${deal.price}</p>
      </div>
    `;

    grid.appendChild(card);
  });
}

function applyFilters() {
  const query = document.getElementById('searchInput').value.toLowerCase();
  const selectedRetailer = document.getElementById('retailerFilter').value;
  const selectedCategory = document.getElementById('categoryFilter').value;

  const filtered = allDeals.filter(deal => {
    const matchesSearch = deal.name.toLowerCase().includes(query) || deal.description.toLowerCase().includes(query);
    const matchesRetailer = !selectedRetailer || deal.site === selectedRetailer;
    const matchesCategory = !selectedCategory || deal.category === selectedCategory;
    return matchesSearch && matchesRetailer && matchesCategory;
  });

  renderDeals(filtered);
}

document.getElementById('searchInput').addEventListener('input', applyFilters);
document.getElementById('retailerFilter').addEventListener('change', applyFilters);
document.getElementById('categoryFilter').addEventListener('change', applyFilters);
