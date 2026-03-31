const fetchButton = document.querySelector("#fetch-button");
const statusPill = document.querySelector("#status-pill");
const resultCount = document.querySelector("#result-count");
const lastUpdated = document.querySelector("#last-updated");
const sourceHealth = document.querySelector("#source-health");
const messageBox = document.querySelector("#message-box");
const resultsBody = document.querySelector("#results-body");
const resultsTab = document.querySelector("#tab-results");
const actionedTab = document.querySelector("#tab-actioned");
const expiredTab = document.querySelector("#tab-expired");
const blockedTab = document.querySelector("#tab-blocked");
const resultsPanel = document.querySelector("#panel-results");
const blockedPanel = document.querySelector("#panel-blocked");
const resultsSectionLabel = document.querySelector("#results-section-label");
const resultsTitle = document.querySelector("#results-title");
const resultsSummary = document.querySelector("#results-summary");
const tableView = document.querySelector("#table-view");
const cardsView = document.querySelector("#cards-view");
const detailModal = document.querySelector("#detail-modal");
const detailBackdrop = document.querySelector("#detail-backdrop");
const detailCloseButton = document.querySelector("#detail-close");
const detailTitle = document.querySelector("#detail-title");
const detailFit = document.querySelector("#detail-fit");
const detailSource = document.querySelector("#detail-source");
const detailOrganization = document.querySelector("#detail-organization");
const detailCountries = document.querySelector("#detail-countries");
const detailDeadline = document.querySelector("#detail-deadline");
const detailType = document.querySelector("#detail-type");
const detailStatus = document.querySelector("#detail-status");
const detailExistingNotes = document.querySelector("#detail-existing-notes");
const detailNotes = document.querySelector("#detail-notes");
const detailLink = document.querySelector("#detail-link");
const detailActionApplied = document.querySelector("#detail-action-applied");
const detailActionNotInterested = document.querySelector("#detail-action-not-interested");
const detailActionNotRelevant = document.querySelector("#detail-action-not-relevant");
const detailActionReset = document.querySelector("#detail-action-reset");
const tablePagination = document.querySelector("#table-pagination");
const paginationSummary = document.querySelector("#pagination-summary");
const paginationPage = document.querySelector("#pagination-page");
const paginationPrevButton = document.querySelector("#pagination-prev");
const paginationNextButton = document.querySelector("#pagination-next");
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
const DEBUG_STORAGE_KEY = "fairpicture-opportunities-debug";
let allOpportunities = [];
let currentTablePage = 1;
let currentOpportunityTab = "results";
let selectedOpportunityId = null;
let activeBucketRequestId = 0;
const TABLE_PAGE_SIZE = 10;

fetchButton.addEventListener("click", handleFetch);
resultsTab.addEventListener("click", () => setActiveTab("results"));
actionedTab.addEventListener("click", () => setActiveTab("actioned"));
expiredTab.addEventListener("click", () => setActiveTab("expired"));
blockedTab.addEventListener("click", () => setActiveTab("blocked"));
resultsBody.addEventListener("click", handleOpportunityListClick);
cardsView.addEventListener("click", handleOpportunityListClick);
detailBackdrop.addEventListener("click", closeDetailModal);
detailCloseButton.addEventListener("click", closeDetailModal);
detailActionApplied.addEventListener("click", () => handleDetailActionSubmit("applied"));
detailActionNotInterested.addEventListener("click", () => handleDetailActionSubmit("not_interested"));
detailActionNotRelevant.addEventListener("click", () => handleDetailActionSubmit("not_relevant"));
detailActionReset.addEventListener("click", () => handleDetailActionSubmit(""));
viewTableButton.addEventListener("click", () => setViewMode("table"));
viewCardsButton.addEventListener("click", () => setViewMode("cards"));
themeLightButton.addEventListener("click", () => setTheme("light"));
themeDarkButton.addEventListener("click", () => setTheme("dark"));
tableSearch.addEventListener("input", () => {
  currentTablePage = 1;
  applyTableState();
});
sourceFilter.addEventListener("change", () => {
  currentTablePage = 1;
  applyTableState();
});
sortBy.addEventListener("change", () => {
  currentTablePage = 1;
  applyTableState();
});
paginationPrevButton.addEventListener("click", () => changeTablePage(-1));
paginationNextButton.addEventListener("click", () => changeTablePage(1));
window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !detailModal.hidden) {
    closeDetailModal();
  }
});
window.addEventListener("load", () => {
  initializePreferences();
  initializeDebugMode();
  loadInitialData();
});

async function loadInitialData() {
  fetchButton.disabled = true;
  setStatus("Loading...", "loading");
  setMessage("Loading cached opportunities from the database.", "info");

  try {
    const sync = await fetchSyncStatus();
    updateSyncMeta(sync);
    debugLog("loadInitialData:sync", { currentOpportunityTab, sync });
    await reloadOpportunitiesFromApi();
    setStatus(allOpportunities.length ? "Cached" : "Ready", "success");
  } catch (error) {
    allOpportunities = [];
    applyTableState();
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
    if (currentOpportunityTab === "results") {
      allOpportunities = Array.isArray(refreshResponse?.items) ? refreshResponse.items : [];
      applyTableState();
    } else {
      await reloadOpportunitiesFromApi({ preserveMessage: true });
    }
    updateSyncMeta(refreshResponse?.sync || null, refreshResponse?.sources || null);
    const failedSourceCount = Array.isArray(refreshResponse?.sources)
      ? refreshResponse.sources.filter((source) => source?.status === "failed").length
      : 0;
    setMessage(
      failedSourceCount
        ? `Refresh completed with ${failedSourceCount} source issue${failedSourceCount === 1 ? "" : "s"}. ${refreshResponse?.newCount || 0} new and ${refreshResponse?.updatedCount || 0} updated opportunities.`
        : `Refresh completed. ${refreshResponse?.newCount || 0} new and ${refreshResponse?.updatedCount || 0} updated opportunities.`,
      failedSourceCount ? "info" : "success"
    );
    setStatus(failedSourceCount ? "Partial" : "Updated", "success");
  } catch (error) {
    setMessage(error.message || "Refresh failed.", "error");
    setStatus("Error", "error");
  } finally {
    fetchButton.disabled = false;
  }
}

async function fetchOpportunities(options = {}) {
  const bucket = options.bucket || getApiBucketForTab(currentOpportunityTab);
  const url = new URL("/api/opportunities", window.location.origin);
  url.searchParams.set("bucket", bucket);
  if (options.noStore) {
    url.searchParams.set("ts", String(Date.now()));
  }
  const response = await fetch(url.toString(), options.noStore ? { cache: "no-store" } : undefined);
  const responseData = await response.json().catch(() => null);
  debugLog("fetchOpportunities:response", {
    bucket,
    url: url.toString(),
    ok: response.ok,
    status: response.status,
    itemCount: Array.isArray(responseData?.items) ? responseData.items.length : null,
    sampleStatuses: Array.isArray(responseData?.items)
      ? responseData.items.slice(0, 5).map((item) => ({
          id: item?.id,
          title: item?.title,
          actionStatus: item?.actionStatus,
          status: item?.status,
        }))
      : [],
  });

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

async function saveOpportunityAction(id, actionStatus, notes = "") {
  const response = await fetch("/api/opportunity-action", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ id, actionStatus, notes }),
  });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not save the opportunity action.");
  }

  return responseData?.item || null;
}

async function reloadOpportunitiesFromApi(options = {}) {
  const requestId = ++activeBucketRequestId;
  const bucket = options.bucket || getApiBucketForTab(currentOpportunityTab);
  debugLog("reload:start", { requestId, bucket, currentOpportunityTab, options });

  allOpportunities = [];
  currentTablePage = 1;
  resetResultsViewForCurrentTab();

  if (!options.preserveMessage) {
    setMessage("Loading cached opportunities for this view.", "info");
  }

  const opportunities = await fetchOpportunities({
    noStore: true,
    bucket,
  });

  if (requestId !== activeBucketRequestId || bucket !== getApiBucketForTab(currentOpportunityTab)) {
    debugLog("reload:ignored-stale-response", {
      requestId,
      activeBucketRequestId,
      responseBucket: bucket,
      currentBucket: getApiBucketForTab(currentOpportunityTab),
    });
    return;
  }

  allOpportunities = opportunities;
  debugLog("reload:apply", {
    requestId,
    bucket,
    appliedCount: opportunities.length,
  });
  applyTableState();

  if (!options.preserveMessage) {
    setMessage(getLoadedBucketMessage(bucket, opportunities.length), opportunities.length ? "success" : "info");
  }
}

function getLoadedBucketMessage(bucket, count) {
  if (bucket === "actioned") {
    return count
      ? "Showing cached opportunities with actions taken."
      : "No actioned opportunities yet.";
  }

  if (bucket === "expired") {
    return count
      ? "Showing expired opportunities from the cached database."
      : "No expired opportunities in the current cache.";
  }

  return count
    ? "Showing the latest cached opportunities."
    : "No cached opportunities yet. Click refresh to run a new sync.";
}

function resetResultsViewForCurrentTab() {
  const config = getResultTabConfig(currentOpportunityTab);
  resultsSectionLabel.textContent = config.label;
  resultsTitle.textContent = config.title;
  resultsSummary.textContent = config.summary(0);
  resultCount.textContent = "0 items";
  renderTable([]);
  renderCards([]);
  updatePagination(0, 0, 1);
}

function applyTableState() {
  const filtered = filterOpportunities(
    allOpportunities,
    sourceFilter.value,
    tableSearch.value
  );
  const sorted = sortOpportunities(filtered, sortBy.value);
  const totalPages = Math.max(1, Math.ceil(sorted.length / TABLE_PAGE_SIZE));
  currentTablePage = Math.min(currentTablePage, totalPages);
  const paginated = paginateOpportunities(sorted, currentTablePage, TABLE_PAGE_SIZE);
  updateResultsHeader(sorted.length);
  renderResults(sorted, paginated);
  updatePagination(sorted.length, paginated.length, totalPages);
  resultCount.textContent = `${sorted.length} ${sorted.length === 1 ? "item" : "items"}`;
  debugLog("applyTableState", {
    tab: currentOpportunityTab,
    sourceFilter: sourceFilter.value,
    search: tableSearch.value,
    sortBy: sortBy.value,
    totalItems: allOpportunities.length,
    filteredCount: filtered.length,
    sortedCount: sorted.length,
    pageCount: paginated.length,
    firstIds: sorted.slice(0, 5).map((item) => item.id),
    firstStatuses: sorted.slice(0, 5).map((item) => ({
      id: item.id,
      actionStatus: item.actionStatus,
      status: item.status,
    })),
  });
}

function updateResultsHeader(count) {
  const config = getResultTabConfig(currentOpportunityTab);
  resultsSectionLabel.textContent = config.label;
  resultsTitle.textContent = config.title;
  resultsSummary.textContent = config.summary(count);
}

function paginateOpportunities(opportunities, page, pageSize) {
  const safePage = Math.max(1, page);
  const startIndex = (safePage - 1) * pageSize;
  return opportunities.slice(startIndex, startIndex + pageSize);
}

function updatePagination(totalCount, pageCount, totalPages) {
  const hasPagination = totalCount > TABLE_PAGE_SIZE;
  tablePagination.hidden = !hasPagination;

  if (!hasPagination) {
    return;
  }

  const start = totalCount === 0 ? 0 : (currentTablePage - 1) * TABLE_PAGE_SIZE + 1;
  const end = totalCount === 0 ? 0 : start + pageCount - 1;
  paginationSummary.textContent = `Showing ${start}-${end} of ${totalCount}`;
  paginationPage.textContent = `Page ${currentTablePage} of ${totalPages}`;
  paginationPrevButton.disabled = currentTablePage <= 1;
  paginationNextButton.disabled = currentTablePage >= totalPages;
}

function changeTablePage(delta) {
  currentTablePage = Math.max(1, currentTablePage + delta);
  applyTableState();
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
      opportunity.actionStatus,
      opportunity.actionNotes,
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
      '<tr class="placeholder-row"><td colspan="6">No results to display.</td></tr>';
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
      const safeLink = escapeAttribute(opportunity.link || "");
      const safeSource = escapeHtml(opportunity.source || "Source");
      const safeActionSummary = escapeHtml(getListSummary(opportunity));
      const safeStatus = escapeHtml(getStatusLabel(opportunity));
      const safeFitTooltip = escapeAttribute(
        (Array.isArray(opportunity.fitReasons) && opportunity.fitReasons.length > 0
          ? opportunity.fitReasons
          : ["Stored Fairpicture fit score from the latest sync."]).join(" | ")
      );
      const safeId = escapeAttribute(opportunity.id || "");

      return `
        <tr class="clickable-row" data-opportunity-id="${safeId}">
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
            <span class="cell-subtext">${safeActionSummary}</span>
          </td>
          <td data-label="Source"><span class="type-tag">${safeSource}</span></td>
          <td data-label="Deadline">${safeDeadline}</td>
          <td data-label="Status"><span class="type-tag">${safeStatus}</span></td>
          <td data-label="Link">
            ${
              safeLink
                ? `<a class="result-link" href="${safeLink}" target="_blank" rel="noreferrer" data-stop-row-open="true">Open posting</a>`
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
      const safeActionSummary = escapeHtml(getListSummary(opportunity));
      const safeStatus = escapeHtml(getStatusLabel(opportunity));
      const safeFitTooltip = escapeAttribute(
        (Array.isArray(opportunity.fitReasons) && opportunity.fitReasons.length > 0
          ? opportunity.fitReasons
          : ["Stored Fairpicture fit score from the latest sync."]).join(" | ")
      );
      const safeId = escapeAttribute(opportunity.id || "");

      return `
        <article class="opportunity-card opportunity-card--${fitTone}" data-opportunity-id="${safeId}">
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
            <p class="cell-subtext">${safeActionSummary}</p>
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
            <div>
              <dt>Status</dt>
              <dd><span class="type-tag">${safeStatus}</span></dd>
            </div>
          </dl>

          <div class="opportunity-card__footer">
            ${
              safeLink
                ? `<a class="result-link" href="${safeLink}" target="_blank" rel="noreferrer" data-stop-row-open="true">Open posting</a>`
                : '<span class="cell-subtext">No link provided</span>'
            }
          </div>
        </article>
      `;
    })
    .join("");
}

function renderResults(opportunities, paginatedTableOpportunities) {
  renderTable(paginatedTableOpportunities);
  renderCards(opportunities);
}

function getListSummary(opportunity) {
  if (currentOpportunityTab === "actioned" && opportunity.actionStatus) {
    const takenAt = opportunity.actionTakenAt ? ` on ${formatTimestamp(opportunity.actionTakenAt)}` : "";
    return `${getActionLabel(opportunity.actionStatus)}${takenAt}`;
  }

  if (currentOpportunityTab === "expired") {
    return "Expired tender from the cached database.";
  }

  return "Keyword match from the job title or procurement notice.";
}

function getStatusLabel(opportunity) {
  if (opportunity.status === "expired") {
    return "Expired";
  }

  if (opportunity.actionStatus) {
    return getActionLabel(opportunity.actionStatus);
  }

  return "Live";
}

function getActionLabel(actionStatus) {
  if (actionStatus === "applied") {
    return "Applied";
  }

  if (actionStatus === "not_interested") {
    return "Not interested";
  }

  if (actionStatus === "not_relevant") {
    return "Not relevant";
  }

  return "Action taken";
}

function getResultTabConfig(tab) {
  if (tab === "actioned") {
    return {
      label: "Action taken",
      title: "Workflow",
      summary: (count) =>
        count
          ? `${count} opportunities have already been processed by ops and are hidden from the live queue.`
          : "Items marked applied, not interested, or not relevant will appear here.",
    };
  }

  if (tab === "expired") {
    return {
      label: "Expired tenders",
      title: "Archive",
      summary: (count) =>
        count
          ? `${count} expired tenders are kept here for reference.`
          : "Expired tenders are separated from live work but remain searchable here.",
    };
  }

  return {
    label: "Current matches",
    title: "Results",
    summary: (count) =>
      count
        ? "Prioritized for visual storytelling, production, and procurement relevance."
        : "No live opportunities currently match the active filters.",
  };
}

function getApiBucketForTab(tab) {
  if (tab === "actioned") {
    return "actioned";
  }

  if (tab === "expired") {
    return "expired";
  }

  return "live";
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
  if (!value) {
    return "Unknown time";
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    debugLog("formatTimestamp:invalid", { value });
    return String(value);
  }

  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
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

function updateSyncMeta(sync, sourceResultsOverride = null) {
  if (!sync || !sync.lastSyncedAt) {
    lastUpdated.textContent = "Last synced: never";
    renderSourceHealth([]);
    return;
  }

  lastUpdated.textContent = `Last synced ${formatRelativeTime(sync.lastSyncedAt)}`;
  renderSourceHealth(sourceResultsOverride || sync.sourceResults || []);
}

function renderSourceHealth(sourceResults) {
  if (!sourceHealth) {
    return;
  }

  if (!Array.isArray(sourceResults) || sourceResults.length === 0) {
    sourceHealth.hidden = true;
    sourceHealth.innerHTML = "";
    return;
  }

  sourceHealth.hidden = false;
  sourceHealth.innerHTML = sourceResults
    .map((result) => {
      const tone = getSourceHealthTone(result);
      const sourceName = escapeHtml(result?.source || "Unknown source");
      const itemCount = Number(result?.itemCount) || 0;
      const itemLabel = `${itemCount} ${itemCount === 1 ? "item" : "items"}`;
      const errorMessage = result?.errorMessage ? escapeHtml(result.errorMessage) : "";

      return `
        <div class="source-health-pill source-health-pill--${tone}">
          <span class="source-health-pill__name">${sourceName}</span>
          <span class="source-health-pill__meta">
            ${
              tone === "failed"
                ? errorMessage || "Failed"
                : tone === "skipped"
                  ? "Skipped"
                  : itemLabel
            }
          </span>
        </div>
      `;
    })
    .join("");
}

function getSourceHealthTone(result) {
  if (result?.status === "failed") {
    return "failed";
  }

  if (result?.status === "skipped") {
    return "skipped";
  }

  return (Number(result?.itemCount) || 0) > 0 ? "healthy" : "empty";
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
  activeBucketRequestId += 1;
  currentOpportunityTab = tab;
  const showBlocked = tab === "blocked";
  debugLog("setActiveTab", {
    tab,
    showBlocked,
    activeBucketRequestId,
    currentBucket: getApiBucketForTab(tab),
  });
  closeDetailModal();

  resultsTab.classList.toggle("active", tab === "results");
  actionedTab.classList.toggle("active", tab === "actioned");
  expiredTab.classList.toggle("active", tab === "expired");
  blockedTab.classList.toggle("active", showBlocked);

  resultsTab.setAttribute("aria-selected", String(tab === "results"));
  actionedTab.setAttribute("aria-selected", String(tab === "actioned"));
  expiredTab.setAttribute("aria-selected", String(tab === "expired"));
  blockedTab.setAttribute("aria-selected", String(showBlocked));

  resultsPanel.hidden = showBlocked;
  blockedPanel.hidden = !showBlocked;
  currentTablePage = 1;

  if (!showBlocked) {
    reloadOpportunitiesFromApi().catch(() => {
      debugLog("setActiveTab:reload-failed", { tab });
      applyTableState();
    });
  }
}

function initializeDebugMode() {
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.get("debug") === "1") {
      localStorage.setItem(DEBUG_STORAGE_KEY, "1");
    } else if (params.get("debug") === "0") {
      localStorage.removeItem(DEBUG_STORAGE_KEY);
    }
  } catch (error) {
    return;
  }
}

function isDebugEnabled() {
  try {
    return localStorage.getItem(DEBUG_STORAGE_KEY) === "1";
  } catch (error) {
    return false;
  }
}

function debugLog(event, payload) {
  if (!isDebugEnabled()) {
    return;
  }

  console.log(`[Fairpicture Debug] ${event}`, payload);
}

function handleOpportunityListClick(event) {
  if (event.target.closest("[data-stop-row-open='true']")) {
    return;
  }

  const row = event.target.closest("[data-opportunity-id]");
  if (!row) {
    return;
  }

  openDetailModal(row.getAttribute("data-opportunity-id"));
}

function openDetailModal(opportunityId) {
  const opportunity = allOpportunities.find((item) => item.id === opportunityId);
  if (!opportunity) {
    return;
  }

  selectedOpportunityId = opportunityId;
  const fitScore = Number.isFinite(opportunity.fitScore) ? opportunity.fitScore : 0;
  const fitTone = getFitTone(fitScore);

  detailTitle.textContent = opportunity.title || "Opportunity";
  detailFit.className = `fit-score fit-score--${fitTone}`;
  detailFit.innerHTML = `<strong>${fitScore}%</strong><span>${escapeHtml(opportunity.fitLabel || getFitLabel(fitScore))}</span>`;
  detailSource.textContent = opportunity.source || "Source";
  detailOrganization.textContent = opportunity.organization || "N/A";
  detailCountries.textContent =
    Array.isArray(opportunity.countryList) && opportunity.countryList.length > 0
      ? opportunity.countryList.join(", ")
      : "Global / unspecified";
  detailDeadline.textContent = formatDate(opportunity.deadline);
  detailType.textContent = opportunity.type || "Opportunity";
  detailStatus.textContent = getStatusLabel(opportunity);
  detailExistingNotes.textContent = opportunity.actionNotes || "No notes yet.";
  detailNotes.value = opportunity.actionNotes || "";
  detailLink.href = opportunity.link || "#";
  detailLink.hidden = !opportunity.link;
  const isLiveTab = currentOpportunityTab === "results";
  const isActionedTab = currentOpportunityTab === "actioned";
  const isExpiredTab = currentOpportunityTab === "expired";

  detailNotes.disabled = isExpiredTab;
  detailActionApplied.hidden = !isLiveTab;
  detailActionNotInterested.hidden = !isLiveTab;
  detailActionNotRelevant.hidden = !isLiveTab;
  detailActionReset.hidden = !isActionedTab;
  detailModal.hidden = false;
  document.body.classList.add("modal-open");
}

function closeDetailModal() {
  detailModal.hidden = true;
  selectedOpportunityId = null;
  document.body.classList.remove("modal-open");
}

async function handleDetailActionSubmit(actionStatus) {
  if (!selectedOpportunityId) {
    return;
  }

  const notes = detailNotes.value.trim();
  const isReset = !actionStatus;

  if (!isReset && !notes) {
    setMessage("Notes are required before taking an action.", "error");
    detailNotes.focus();
    return;
  }

  toggleDetailActionButtons(true);

  try {
    const updatedItem = await saveOpportunityAction(selectedOpportunityId, actionStatus, notes);
    if (updatedItem) {
      allOpportunities = allOpportunities.map((item) =>
        item.id === updatedItem.id ? updatedItem : item
      );
      await reloadOpportunitiesFromApi();
      setMessage(
        isReset
          ? "Opportunity moved back to the live queue."
          : `Opportunity marked as ${getActionLabel(actionStatus).toLowerCase()}.`,
        "success"
      );
      closeDetailModal();
    }
  } catch (error) {
    setMessage(error.message || "Could not save the opportunity action.", "error");
  } finally {
    toggleDetailActionButtons(false);
  }
}

function toggleDetailActionButtons(disabled) {
  [
    detailActionApplied,
    detailActionNotInterested,
    detailActionNotRelevant,
    detailActionReset,
  ].forEach((button) => {
    button.disabled = disabled;
  });
}

function setTheme(theme, options = {}) {
  const nextTheme = theme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = nextTheme;
  document.documentElement.style.colorScheme = nextTheme;
  document.body.dataset.theme = nextTheme;
  themeLightButton.classList.toggle("active", nextTheme === "light");
  themeDarkButton.classList.toggle("active", nextTheme === "dark");

  if (options.persist !== false) {
    localStorage.setItem(STORAGE_KEYS.theme, nextTheme);
  }
}

function setViewMode(view, options = {}) {
  const nextView = view === "cards" ? "cards" : "table";
  document.documentElement.dataset.view = nextView;
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
