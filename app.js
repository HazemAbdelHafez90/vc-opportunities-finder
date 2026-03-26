const fetchButton = document.querySelector("#fetch-button");
const statusPill = document.querySelector("#status-pill");
const resultCount = document.querySelector("#result-count");
const lastUpdated = document.querySelector("#last-updated");
const messageBox = document.querySelector("#message-box");
const resultsBody = document.querySelector("#results-body");
const resultsTab = document.querySelector("#tab-results");
const blockedTab = document.querySelector("#tab-blocked");
const resultsPanel = document.querySelector("#panel-results");
const blockedPanel = document.querySelector("#panel-blocked");
const tableView = document.querySelector("#table-view");
const cardsView = document.querySelector("#cards-view");
const tableSearch = document.querySelector("#table-search");
const sourceFilter = document.querySelector("#source-filter");
const sortBy = document.querySelector("#sort-by");
const viewTableButton = document.querySelector("#view-table");
const viewCardsButton = document.querySelector("#view-cards");
const themeLightButton = document.querySelector("#theme-light");
const themeDarkButton = document.querySelector("#theme-dark");
const STORAGE_KEYS = {
  theme: "fairpicture-opportunities-theme",
  view: "fairpicture-opportunities-view",
};
let allOpportunities = [];

fetchButton.addEventListener("click", handleFetch);
resultsTab.addEventListener("click", () => setActiveTab("results"));
blockedTab.addEventListener("click", () => setActiveTab("blocked"));
viewTableButton.addEventListener("click", () => setViewMode("table"));
viewCardsButton.addEventListener("click", () => setViewMode("cards"));
themeLightButton.addEventListener("click", () => setTheme("light"));
themeDarkButton.addEventListener("click", () => setTheme("dark"));
tableSearch.addEventListener("input", applyTableState);
sourceFilter.addEventListener("change", applyTableState);
sortBy.addEventListener("change", applyTableState);
window.addEventListener("load", () => {
  initializePreferences();
  loadInitialData();
});

async function loadInitialData() {
  fetchButton.disabled = true;
  setStatus("Loading...", "loading");
  setMessage("Loading cached opportunities from the database.", "info");

  try {
    const [opportunities, sync] = await Promise.all([
      fetchOpportunities(),
      fetchSyncStatus(),
    ]);

    allOpportunities = opportunities;
    applyTableState();
    updateSyncMeta(sync);
    setStatus(opportunities.length ? "Cached" : "Ready", "success");
    setMessage(
      opportunities.length
        ? "Showing the latest cached opportunities."
        : "No cached opportunities yet. Click refresh to run a new sync.",
      opportunities.length ? "success" : "info"
    );
  } catch (error) {
    allOpportunities = [];
    renderTable([]);
    resultCount.textContent = "0 items";
    lastUpdated.textContent = "Sync status unavailable";
    setMessage(error.message || "Could not load cached opportunities.", "error");
    setStatus("Error", "error");
  } finally {
    fetchButton.disabled = false;
  }
}

async function handleFetch() {
  fetchButton.disabled = true;
  setStatus("Syncing...", "loading");
  setMessage("Checking live sources and saving fresh opportunities into the database.", "info");

  try {
    const refreshResponse = await refreshOpportunities();
    allOpportunities = Array.isArray(refreshResponse?.items) ? refreshResponse.items : [];
    applyTableState();
    updateSyncMeta(refreshResponse?.sync || null);
    setMessage(
      `Refresh completed. ${refreshResponse?.newCount || 0} new and ${refreshResponse?.updatedCount || 0} updated opportunities.`,
      "success"
    );
    setStatus("Updated", "success");
  } catch (error) {
    setMessage(error.message || "Refresh failed.", "error");
    setStatus("Error", "error");
  } finally {
    fetchButton.disabled = false;
  }
}

async function fetchOpportunities() {
  const response = await fetch("/api/opportunities");
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "The cached opportunities request failed.");
  }

  return Array.isArray(responseData?.items) ? responseData.items : [];
}

async function fetchSyncStatus() {
  const response = await fetch("/api/sync-status");
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not load sync status.");
  }

  return responseData?.sync || null;
}

async function refreshOpportunities() {
  const response = await fetch("/api/refresh", { method: "POST" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "The refresh request failed.");
  }

  return responseData || {};
}

function applyTableState() {
  const filtered = filterOpportunities(
    allOpportunities,
    sourceFilter.value,
    tableSearch.value
  );
  const sorted = sortOpportunities(filtered, sortBy.value);
  renderResults(sorted);
  resultCount.textContent = `${sorted.length} ${sorted.length === 1 ? "item" : "items"}`;
}

function filterOpportunities(opportunities, source, searchTerm) {
  const normalizedSearch = String(searchTerm || "").trim().toLowerCase();

  return opportunities.filter((opportunity) => {
    const matchesSource = source === "all" || opportunity.source === source;
    if (!matchesSource) {
      return false;
    }

    if (!normalizedSearch) {
      return true;
    }

    const haystack = [
      opportunity.title,
      opportunity.source,
      opportunity.organization,
      opportunity.type,
      Array.isArray(opportunity.countryList) ? opportunity.countryList.join(" ") : "",
    ]
      .join(" ")
      .toLowerCase();

    return haystack.includes(normalizedSearch);
  });
}

function sortOpportunities(opportunities, mode) {
  const items = [...opportunities];

  items.sort((left, right) => {
    if (mode === "fit-desc") {
      return (Number(right.fitScore) || 0) - (Number(left.fitScore) || 0) || safeCompare(left.title, right.title);
    }

    if (mode === "title-asc") {
      return safeCompare(left.title, right.title);
    }

    if (mode === "source-asc") {
      return safeCompare(left.source, right.source) || safeCompare(left.title, right.title);
    }

    const leftTime = parseSortableDate(left.deadline);
    const rightTime = parseSortableDate(right.deadline);

    if (mode === "deadline-desc") {
      return rightTime - leftTime || safeCompare(left.title, right.title);
    }

    return leftTime - rightTime || safeCompare(left.title, right.title);
  });

  return items;
}

function renderTable(opportunities) {
  if (opportunities.length === 0) {
    resultsBody.innerHTML =
      '<tr class="placeholder-row"><td colspan="8">No results to display.</td></tr>';
    return;
  }

  resultsBody.innerHTML = opportunities
    .map((opportunity) => {
      const fitScore = Number.isFinite(opportunity.fitScore) ? opportunity.fitScore : 0;
      const fitLabel = opportunity.fitLabel || getFitLabel(fitScore);
      const fitTone = getFitTone(fitScore);
      const safeTitle = escapeHtml(opportunity.title);
      const safeOrganization = escapeHtml(opportunity.organization || "N/A");
      const safeCountries = escapeHtml(
        Array.isArray(opportunity.countryList) && opportunity.countryList.length > 0
          ? opportunity.countryList.join(", ")
          : "Global / unspecified"
      );
      const safeDeadline = escapeHtml(formatDate(opportunity.deadline));
      const safeType = escapeHtml(opportunity.type || "Opportunity");
      const safeLink = escapeAttribute(opportunity.link || "");
      const safeSource = escapeHtml(opportunity.source || "Source");
      const safeFitTooltip = escapeAttribute(
        (Array.isArray(opportunity.fitReasons) && opportunity.fitReasons.length > 0
          ? opportunity.fitReasons
          : ["Stored Fairpicture fit score from the latest sync."]).join(" | ")
      );

      return `
        <tr>
          <td data-label="Fit">
            <div
              class="fit-score fit-score--${fitTone}"
              title="${safeFitTooltip}"
              aria-label="${safeFitTooltip}"
              tabindex="0"
            >
              <strong>${fitScore}%</strong>
              <span>${fitLabel}</span>
            </div>
          </td>
          <td data-label="Opportunity">
            <p class="opportunity-title">${safeTitle}</p>
            <span class="cell-subtext">Keyword match from the job title or procurement notice.</span>
          </td>
          <td data-label="Source"><span class="type-tag">${safeSource}</span></td>
          <td data-label="Organization">${safeOrganization}</td>
          <td data-label="Countries">${safeCountries}</td>
          <td data-label="Deadline">${safeDeadline}</td>
          <td data-label="Type"><span class="type-tag">${safeType}</span></td>
          <td data-label="Link">
            ${
              safeLink
                ? `<a class="result-link" href="${safeLink}" target="_blank" rel="noreferrer">Open posting</a>`
                : '<span class="cell-subtext">No link provided</span>'
            }
          </td>
        </tr>
      `;
    })
    .join("");
}

function renderCards(opportunities) {
  if (opportunities.length === 0) {
    cardsView.innerHTML = '<article class="empty-card">No results to display.</article>';
    return;
  }

  cardsView.innerHTML = opportunities
    .map((opportunity) => {
      const fitScore = Number.isFinite(opportunity.fitScore) ? opportunity.fitScore : 0;
      const fitLabel = opportunity.fitLabel || getFitLabel(fitScore);
      const fitTone = getFitTone(fitScore);
      const safeTitle = escapeHtml(opportunity.title);
      const safeOrganization = escapeHtml(opportunity.organization || "N/A");
      const safeCountries = escapeHtml(
        Array.isArray(opportunity.countryList) && opportunity.countryList.length > 0
          ? opportunity.countryList.join(", ")
          : "Global / unspecified"
      );
      const safeDeadline = escapeHtml(formatDate(opportunity.deadline));
      const safeType = escapeHtml(opportunity.type || "Opportunity");
      const safeLink = escapeAttribute(opportunity.link || "");
      const safeSource = escapeHtml(opportunity.source || "Source");
      const safeFitTooltip = escapeAttribute(
        (Array.isArray(opportunity.fitReasons) && opportunity.fitReasons.length > 0
          ? opportunity.fitReasons
          : ["Stored Fairpicture fit score from the latest sync."]).join(" | ")
      );

      return `
        <article class="opportunity-card opportunity-card--${fitTone}">
          <div class="opportunity-card__top">
            <span class="type-tag">${safeSource}</span>
            <div
              class="fit-score fit-score--${fitTone}"
              title="${safeFitTooltip}"
              aria-label="${safeFitTooltip}"
              tabindex="0"
            >
              <strong>${fitScore}%</strong>
              <span>${fitLabel}</span>
            </div>
          </div>

          <div class="opportunity-card__body">
            <h3>${safeTitle}</h3>
            <p class="cell-subtext">Keyword match from the latest synced opportunity sources.</p>
          </div>

          <dl class="opportunity-card__meta">
            <div>
              <dt>Organization</dt>
              <dd>${safeOrganization}</dd>
            </div>
            <div>
              <dt>Countries</dt>
              <dd>${safeCountries}</dd>
            </div>
            <div>
              <dt>Deadline</dt>
              <dd>${safeDeadline}</dd>
            </div>
            <div>
              <dt>Type</dt>
              <dd><span class="type-tag">${safeType}</span></dd>
            </div>
          </dl>

          <div class="opportunity-card__footer">
            ${
              safeLink
                ? `<a class="result-link" href="${safeLink}" target="_blank" rel="noreferrer">Open posting</a>`
                : '<span class="cell-subtext">No link provided</span>'
            }
          </div>
        </article>
      `;
    })
    .join("");
}

function renderResults(opportunities) {
  renderTable(opportunities);
  renderCards(opportunities);
}

function formatDate(value) {
  if (!value) {
    return "No deadline listed";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(date);
}

function formatTimestamp(value) {
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(value);
}

function setMessage(message, tone) {
  messageBox.className = `message-box ${tone}`;
  messageBox.textContent = message;
}

function setStatus(label, tone) {
  statusPill.className = `status-pill ${tone}`;
  const labelNode = statusPill.querySelector(".status-pill__label");
  if (labelNode) {
    labelNode.textContent = label;
  }
  fetchButton.classList.toggle("loading", tone === "loading");
}

function initializePreferences() {
  const storedTheme = localStorage.getItem(STORAGE_KEYS.theme);
  const storedView = localStorage.getItem(STORAGE_KEYS.view);
  setTheme(storedTheme === "dark" ? "dark" : "light", { persist: false });
  setViewMode(storedView === "cards" ? "cards" : "table", { persist: false });
}

function updateSyncMeta(sync) {
  if (!sync || !sync.lastSyncedAt) {
    lastUpdated.textContent = "Last synced: never";
    return;
  }

  lastUpdated.textContent = `Last synced ${formatRelativeTime(sync.lastSyncedAt)}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll("`", "&#96;");
}

function setActiveTab(tab) {
  const showResults = tab === "results";
  resultsTab.classList.toggle("active", showResults);
  blockedTab.classList.toggle("active", !showResults);
  resultsTab.setAttribute("aria-selected", String(showResults));
  blockedTab.setAttribute("aria-selected", String(!showResults));
  resultsPanel.hidden = !showResults;
  blockedPanel.hidden = showResults;
}

function setTheme(theme, options = {}) {
  const nextTheme = theme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = nextTheme;
  document.body.dataset.theme = nextTheme;
  themeLightButton.classList.toggle("active", nextTheme === "light");
  themeDarkButton.classList.toggle("active", nextTheme === "dark");

  if (options.persist !== false) {
    localStorage.setItem(STORAGE_KEYS.theme, nextTheme);
  }
}

function setViewMode(view, options = {}) {
  const nextView = view === "cards" ? "cards" : "table";
  document.body.dataset.view = nextView;
  viewTableButton.classList.toggle("active", nextView === "table");
  viewCardsButton.classList.toggle("active", nextView === "cards");
  tableView.hidden = nextView !== "table";
  cardsView.hidden = nextView !== "cards";

  if (options.persist !== false) {
    localStorage.setItem(STORAGE_KEYS.view, nextView);
  }
}

function parseSortableDate(value) {
  const date = new Date(value || "");
  return Number.isNaN(date.getTime()) ? Number.MAX_SAFE_INTEGER : date.getTime();
}

function safeCompare(left, right) {
  return String(left || "").localeCompare(String(right || ""), undefined, { sensitivity: "base" });
}

function getFitTone(score) {
  if (score >= 75) {
    return "high";
  }

  if (score >= 50) {
    return "medium";
  }

  return "low";
}

function getFitLabel(score) {
  if (score >= 75) {
    return "High fit";
  }

  if (score >= 50) {
    return "Medium fit";
  }

  return "Low fit";
}

function formatRelativeTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  const diffMs = date.getTime() - Date.now();
  const diffMinutes = Math.round(diffMs / 60000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });

  if (Math.abs(diffMinutes) < 60) {
    return formatter.format(diffMinutes, "minute");
  }

  const diffHours = Math.round(diffMinutes / 60);
  if (Math.abs(diffHours) < 24) {
    return formatter.format(diffHours, "hour");
  }

  const diffDays = Math.round(diffHours / 24);
  return formatter.format(diffDays, "day");
}
