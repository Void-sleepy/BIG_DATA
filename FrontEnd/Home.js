let allDeals = [];  // Store all deals globally

// Fetch deals from JSON file and render them
fetch('airfryer_deals_20250524_150235.json')
  .then(response => response.json())
  .then(data => {
    // Filter deals with discount >= num%
    const filteredData = data.filter(deal => deal.discount_percentage >= 20);

    allDeals = filteredData.map(deal => ({
      name: deal.product_name,
      description: `${deal.category.replace(/_/g, ' ')} • ${deal.discount_percentage}% off`,
      price: `${deal.price.toFixed(2)} ${deal.currency}`,
      site: deal.retailer,
      link: deal.product_url,
      retailer: deal.retailer.toLowerCase()  // for case-insensitive filtering
    }));

    renderDeals(allDeals);
  })
  .catch(error => {
    console.error("Failed to load deals:", error);
  });

function renderDeals(dealList) {
  const grid = document.getElementById('dealGrid');
  grid.innerHTML = '';

  if (dealList.length === 0) {
    grid.innerHTML = '<p style="grid-column: span 4; text-align:center;">No deals found for this retailer.</p>';
    return;
  }

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

// Add event listeners for retailer buttons
document.querySelectorAll('.category-btn').forEach(button => {
  button.addEventListener('click', () => {
    const selectedRetailer = button.textContent.toLowerCase();

    // Filter deals by retailer (case-insensitive)
    const filteredDeals = allDeals.filter(deal => deal.retailer === selectedRetailer);

    renderDeals(filteredDeals);
  });
});
