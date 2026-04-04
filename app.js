const fetchButton = document.querySelector("#fetch-button");
const statusPill = document.querySelector("#status-pill");
const resultCount = document.querySelector("#result-count");
const lastUpdated = document.querySelector("#last-updated");
const sourceHealth = document.querySelector("#source-health");
const messageBox = document.querySelector("#message-box");
const openSettingsButton = document.querySelector("#open-settings");
const notificationSettingsStatus = document.querySelector("#notification-settings-status");
const notificationEmails = document.querySelector("#notification-emails");
const notificationExpiryDays = document.querySelector("#notification-expiry-days");
const notificationSenderName = document.querySelector("#notification-sender-name");
const notificationSenderEmail = document.querySelector("#notification-sender-email");
const notificationEnabled = document.querySelector("#notification-enabled");
const notificationNewEnabled = document.querySelector("#notification-new-enabled");
const notificationExpiryEnabled = document.querySelector("#notification-expiry-enabled");
const notificationSaveButton = document.querySelector("#notification-save");
const notificationTestButton = document.querySelector("#notification-test");
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
const settingsModal = document.querySelector("#settings-modal");
const settingsBackdrop = document.querySelector("#settings-backdrop");
const settingsCloseButton = document.querySelector("#settings-close");
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
const detailActionExpired = document.querySelector("#detail-action-expired");
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
const DISPLAY_SOURCES = ["ReliefWeb", "UNDP Procurement", "UNGM", "ICIMOD"];
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
let notificationSettings = null;
const TABLE_PAGE_SIZE = 10;

fetchButton.addEventListener("click", handleFetch);
openSettingsButton.addEventListener("click", openSettingsModal);
notificationSaveButton.addEventListener("click", handleSaveNotificationSettings);
notificationTestButton.addEventListener("click", handleTestNotification);
resultsTab.addEventListener("click", () => setActiveTab("results"));
actionedTab.addEventListener("click", () => setActiveTab("actioned"));
expiredTab.addEventListener("click", () => setActiveTab("expired"));
blockedTab.addEventListener("click", () => setActiveTab("blocked"));
resultsBody.addEventListener("click", handleOpportunityListClick);
cardsView.addEventListener("click", handleOpportunityListClick);
detailBackdrop.addEventListener("click", closeDetailModal);
detailCloseButton.addEventListener("click", closeDetailModal);
settingsBackdrop.addEventListener("click", closeSettingsModal);
settingsCloseButton.addEventListener("click", closeSettingsModal);
detailActionApplied.addEventListener("click", () => handleDetailActionSubmit("applied"));
detailActionExpired.addEventListener("click", () => handleDetailActionSubmit("expired_manual"));
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
  if (event.key === "Escape" && !settingsModal.hidden) {
    closeSettingsModal();
  } else if (event.key === "Escape" && !detailModal.hidden) {
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
  setNotificationSettingsBusy(true, "Loading notification settings.");
  setStatus("Loading...", "loading");
  setMessage("Loading cached opportunities from the database.", "info");

  try {
    const [sync, settings] = await Promise.all([
      fetchSyncStatus(),
      fetchNotificationSettings(),
    ]);
    applyNotificationSettings(settings);
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
    setNotificationSettingsBusy(false);
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
    const notificationMessage = getNotificationSummaryMessage(refreshResponse?.notifications || null);
    const failedSourceCount = Array.isArray(refreshResponse?.sources)
      ? refreshResponse.sources.filter((source) => source?.status === "failed").length
      : 0;
    setMessage(
      failedSourceCount
        ? `Refresh completed with ${failedSourceCount} source issue${failedSourceCount === 1 ? "" : "s"}. ${refreshResponse?.newCount || 0} new and ${refreshResponse?.updatedCount || 0} updated opportunities. ${notificationMessage}`
        : `Refresh completed. ${refreshResponse?.newCount || 0} new and ${refreshResponse?.updatedCount || 0} updated opportunities. ${notificationMessage}`,
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
  const url = new URL("/api/sync-status", window.location.origin);
  url.searchParams.set("ts", String(Date.now()));
  const response = await fetch(url.toString(), { cache: "no-store" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not load sync status.");
  }

  return responseData?.sync || null;
}

async function fetchNotificationSettings() {
  const url = new URL("/api/notification-settings", window.location.origin);
  url.searchParams.set("ts", String(Date.now()));
  const response = await fetch(url.toString(), { cache: "no-store" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not load notification settings.");
  }

  return responseData?.settings || null;
}

async function saveNotificationSettings(settings) {
  const response = await fetch("/api/notification-settings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(settings),
  });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not save notification settings.");
  }

  return responseData?.settings || null;
}

async function sendTestNotification() {
  const response = await fetch("/api/test-notification", { method: "POST" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not send the test notification.");
  }

  return responseData?.result || null;
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
      '<tr class="placeholder-row"><td colspan="7">No results to display.</td></tr>';
    return;
  }

  resultsBody.innerHTML = opportunities
    .map((opportunity) => {
      const expiringSoon = isExpiringSoon(opportunity);
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
      const safeAddedAt = escapeHtml(formatAddedDate(opportunity.addedAt));
      const safeStatus = escapeHtml(getStatusLabel(opportunity));
      const safeFitTooltip = escapeAttribute(
        (Array.isArray(opportunity.fitReasons) && opportunity.fitReasons.length > 0
          ? opportunity.fitReasons
          : ["Stored Fairpicture fit score from the latest sync."]).join(" | ")
      );
      const safeId = escapeAttribute(opportunity.id || "");

      return `
        <tr class="clickable-row${expiringSoon ? " clickable-row--expiring" : ""}" data-opportunity-id="${safeId}">
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
            <span class="cell-subtext">${escapeHtml(getOpportunityMetaLine(opportunity))}</span>
          </td>
          <td data-label="Source">
            <span class="type-tag">${safeSource}</span>
          </td>
          <td data-label="Added">${safeAddedAt}</td>
          <td data-label="Deadline">${safeDeadline}</td>
          <td data-label="Status"><span class="type-tag${expiringSoon ? " type-tag--expiring" : ""}">${safeStatus}</span></td>
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
      const expiringSoon = isExpiringSoon(opportunity);
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
      const safeAddedAt = escapeHtml(formatAddedDate(opportunity.addedAt));
      const safeType = escapeHtml(opportunity.type || "Opportunity");
      const safeLink = escapeAttribute(opportunity.link || "");
      const safeSource = escapeHtml(opportunity.source || "Source");
      const safeStatus = escapeHtml(getStatusLabel(opportunity));
      const safeFitTooltip = escapeAttribute(
        (Array.isArray(opportunity.fitReasons) && opportunity.fitReasons.length > 0
          ? opportunity.fitReasons
          : ["Stored Fairpicture fit score from the latest sync."]).join(" | ")
      );
      const safeId = escapeAttribute(opportunity.id || "");

      return `
        <article class="opportunity-card opportunity-card--${fitTone}${expiringSoon ? " opportunity-card--expiring" : ""}" data-opportunity-id="${safeId}">
          <div class="opportunity-card__top">
            <div class="opportunity-card__source-tags">
              <span class="type-tag">${safeSource}</span>
            </div>
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
            <p class="cell-subtext">${escapeHtml(getOpportunityMetaLine(opportunity))}</p>
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
              <dt>Added</dt>
              <dd>${safeAddedAt}</dd>
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
              <dd><span class="type-tag${expiringSoon ? " type-tag--expiring" : ""}">${safeStatus}</span></dd>
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

function getOpportunityMetaLine(opportunity) {
  if (currentOpportunityTab === "actioned" && opportunity.actionStatus) {
    return getListSummary(opportunity);
  }

  const parts = [];
  if (opportunity.organization && opportunity.organization !== "N/A") {
    parts.push(opportunity.organization);
  }

  if (isExpiringSoon(opportunity)) {
    const daysUntilDeadline = getDaysUntilDeadline(opportunity.deadline);
    parts.push(daysUntilDeadline === 0 ? "Closes today" : `Closes in ${daysUntilDeadline} day${daysUntilDeadline === 1 ? "" : "s"}`);
  } else {
    parts.push(getListSummary(opportunity));
  }

  return parts.join(" • ");
}

function getStatusLabel(opportunity) {
  if (opportunity.status === "expired") {
    return "Expired";
  }

  if (isExpiringSoon(opportunity)) {
    return "Expiring soon";
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

  if (actionStatus === "expired_manual") {
    return "Expired";
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

function getExpiryAlertDays() {
  const configured = Number(notificationSettings?.expiryAlertDays);
  return Number.isFinite(configured) ? Math.max(0, configured) : 2;
}

function getDaysUntilDeadline(value) {
  if (!value) {
    return null;
  }

  const deadline = new Date(value);
  if (Number.isNaN(deadline.getTime())) {
    return null;
  }

  const today = new Date();
  const todayStart = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const deadlineStart = new Date(deadline.getFullYear(), deadline.getMonth(), deadline.getDate());
  return Math.round((deadlineStart - todayStart) / 86400000);
}

function isExpiringSoon(opportunity) {
  if (!opportunity || opportunity.status === "expired" || opportunity.actionStatus) {
    return false;
  }

  const daysUntilDeadline = getDaysUntilDeadline(opportunity.deadline);
  const expiryAlertDays = getExpiryAlertDays();
  return daysUntilDeadline !== null && daysUntilDeadline >= 0 && daysUntilDeadline <= expiryAlertDays;
}

function formatAddedDate(value) {
  if (!value) {
    return "Unknown";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
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

function applyNotificationSettings(settings) {
  if (!settings) {
    return;
  }

  notificationSettings = settings;
  notificationEmails.value = Array.isArray(settings.recipientEmails)
    ? settings.recipientEmails.join(", ")
    : "";
  notificationExpiryDays.value = Number.isFinite(settings.expiryAlertDays)
    ? String(settings.expiryAlertDays)
    : "2";
  notificationSenderName.value = settings.senderName || "";
  notificationSenderEmail.value = settings.senderEmail || "";
  notificationEnabled.checked = Boolean(settings.enabled);
  notificationNewEnabled.checked = Boolean(settings.newTenderEnabled);
  notificationExpiryEnabled.checked = Boolean(settings.expiryAlertEnabled);
  notificationSettingsStatus.textContent = getNotificationSettingsStatusText(settings);
}

function collectNotificationSettingsForm() {
  return {
    enabled: notificationEnabled.checked,
    newTenderEnabled: notificationNewEnabled.checked,
    expiryAlertEnabled: notificationExpiryEnabled.checked,
    recipientEmails: splitRecipientEmails(notificationEmails.value),
    senderName: notificationSenderName.value.trim(),
    senderEmail: notificationSenderEmail.value.trim(),
    expiryAlertDays: Number(notificationExpiryDays.value || 0),
  };
}

async function handleSaveNotificationSettings() {
  setNotificationSettingsBusy(true, "Saving notification settings.");

  try {
    const saved = await saveNotificationSettings(collectNotificationSettingsForm());
    applyNotificationSettings(saved);
    setMessage("Notification settings saved.", "success");
  } catch (error) {
    setMessage(error.message || "Could not save notification settings.", "error");
    notificationSettingsStatus.textContent = error.message || "Could not save notification settings.";
  } finally {
    setNotificationSettingsBusy(false);
  }
}

async function handleTestNotification() {
  setNotificationSettingsBusy(true, "Sending test notification.");

  try {
    const result = await sendTestNotification();
    setMessage(result?.message || "Test notification email sent.", "success");
    notificationSettingsStatus.textContent = result?.message || "Test notification email sent.";
  } catch (error) {
    setMessage(error.message || "Could not send the test notification.", "error");
    notificationSettingsStatus.textContent = error.message || "Could not send the test notification.";
  } finally {
    setNotificationSettingsBusy(false);
  }
}

function splitRecipientEmails(value) {
  return String(value || "")
    .split(/[\n,;]+/)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function setNotificationSettingsBusy(isBusy, statusText = null) {
  [
    notificationEmails,
    notificationExpiryDays,
    notificationSenderName,
    notificationSenderEmail,
    notificationEnabled,
    notificationNewEnabled,
    notificationExpiryEnabled,
    notificationSaveButton,
    notificationTestButton,
  ].forEach((element) => {
    element.disabled = isBusy;
  });

  if (statusText) {
    notificationSettingsStatus.textContent = statusText;
  } else if (notificationSettings) {
    notificationSettingsStatus.textContent = getNotificationSettingsStatusText(notificationSettings);
  }
}

function getNotificationSettingsStatusText(settings) {
  if (!settings.enabled) {
    return "Notifications are disabled.";
  }

  const recipientCount = Array.isArray(settings.recipientEmails) ? settings.recipientEmails.length : 0;
  return recipientCount
    ? `Sending to ${recipientCount} recipient${recipientCount === 1 ? "" : "s"}. Expiry alarm: ${settings.expiryAlertDays} day${settings.expiryAlertDays === 1 ? "" : "s"} before deadline.`
    : "Notifications enabled, but no recipients are configured yet.";
}

function getNotificationSummaryMessage(summary) {
  if (!summary) {
    return "Notification status unavailable.";
  }

  const sentParts = [];
  if (Number(summary.newTenderSentCount) > 0) {
    sentParts.push(`${summary.newTenderSentCount} new-tender email match${summary.newTenderSentCount === 1 ? "" : "es"}`);
  }
  if (Number(summary.expiryAlertSentCount) > 0) {
    sentParts.push(`${summary.expiryAlertSentCount} expiry alert${summary.expiryAlertSentCount === 1 ? "" : "s"}`);
  }
  if (Number(summary.expiredTenderSentCount) > 0) {
    sentParts.push(`${summary.expiredTenderSentCount} expired tender alert${summary.expiredTenderSentCount === 1 ? "" : "s"}`);
  }

  if (sentParts.length > 0) {
    return `Notifications sent for ${sentParts.join(" and ")}.`;
  }

  return summary.skippedReason || "No notifications were sent.";
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

  const visibleResults = Array.isArray(sourceResults)
    ? sourceResults.filter((result) => DISPLAY_SOURCES.includes(result?.source))
    : [];

  if (visibleResults.length === 0) {
    sourceHealth.hidden = true;
    sourceHealth.innerHTML = "";
    return;
  }

  sourceHealth.hidden = false;
  sourceHealth.innerHTML = visibleResults
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
  detailActionExpired.hidden = !isLiveTab;
  detailActionNotInterested.hidden = !isLiveTab;
  detailActionNotRelevant.hidden = !isLiveTab;
  detailActionReset.hidden = !isActionedTab;
  detailModal.hidden = false;
  syncGlobalModalState();
}

function closeDetailModal() {
  detailModal.hidden = true;
  selectedOpportunityId = null;
  syncGlobalModalState();
}

function openSettingsModal() {
  settingsModal.hidden = false;
  syncGlobalModalState();
  notificationEmails.focus();
}

function closeSettingsModal() {
  settingsModal.hidden = true;
  syncGlobalModalState();
}

function syncGlobalModalState() {
  document.body.classList.toggle("modal-open", !detailModal.hidden || !settingsModal.hidden);
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
    detailActionExpired,
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
