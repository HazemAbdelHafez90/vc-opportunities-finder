const bootShell = document.querySelector("#boot-shell");
const bootMessage = document.querySelector("#boot-message");
const authShell = document.querySelector("#auth-shell");
const appShell = document.querySelector("#app-shell");
const authForm = document.querySelector("#auth-form");
const authTitle = document.querySelector("#auth-title");
const authSummary = document.querySelector("#auth-summary");
const authEmailField = document.querySelector("#auth-email-field");
const authEmailInput = document.querySelector("#auth-email");
const authPasswordLabel = document.querySelector("#auth-password-label");
const authPasswordInput = document.querySelector("#auth-password");
const authPasswordConfirmField = document.querySelector("#auth-password-confirm-field");
const authPasswordConfirmInput = document.querySelector("#auth-password-confirm");
const authSubmitButton = document.querySelector("#auth-submit");
const authMessage = document.querySelector("#auth-message");
const authUser = document.querySelector("#auth-user");
const authUserEmail = document.querySelector("#auth-user-email");
const signOutButton = document.querySelector("#sign-out-button");
const fetchButton = document.querySelector("#fetch-button");
const statusPill = document.querySelector("#status-pill");
const resultCount = document.querySelector("#result-count");
const lastUpdated = document.querySelector("#last-updated");
const toolbarRange = document.querySelector("#toolbar-range");
const statTotal = document.querySelector("#stat-total");
const statHighFit = document.querySelector("#stat-high-fit");
const statHighFitSub = document.querySelector("#stat-high-fit-sub");
const statExpiring = document.querySelector("#stat-expiring");
const statApplied = document.querySelector("#stat-applied");
const sourceHealth = document.querySelector("#source-health");
const messageBox = document.querySelector("#message-box");
const openSettingsButton = document.querySelector("#open-settings");
const settingsSummaryButton = document.querySelector("#settings-summary-button");
const sourcesButton = document.querySelector("#sources-button");
const sourcesDrawer = document.querySelector("#sources-drawer");
const sourcesDrawerBackdrop = document.querySelector("#sources-drawer-backdrop");
const sourcesCloseButton = document.querySelector("#sources-close");
const notificationSettingsStatus = document.querySelector("#notification-settings-status");
const notificationEmails = document.querySelector("#notification-emails");
const notificationEmailChips = document.querySelector("#notification-email-chips");
const notificationEmailInput = document.querySelector("#notification-email-input");
const notificationEmailError = document.querySelector("#notification-email-error");
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
const pendingTab = document.querySelector("#tab-pending");
const appliedTab = document.querySelector("#tab-applied");
const missedTab = document.querySelector("#tab-missed");
const resultsTabCount = document.querySelector("#tab-results-count");
const appliedTabCount = document.querySelector("#tab-applied-count");
const missedTabCount = document.querySelector("#tab-missed-count");
const resultsPanel = document.querySelector("#panel-results");
const resultsSectionLabel = document.querySelector("#results-section-label");
const resultsTitle = document.querySelector("#results-title");
const resultsSummary = document.querySelector("#results-summary");
const resultsFilterSummary = document.querySelector("#results-filter-summary");
const tableView = document.querySelector("#table-view");
const cardsView = document.querySelector("#cards-view");
const detailModal = document.querySelector("#detail-modal");
const detailBackdrop = document.querySelector("#detail-backdrop");
const detailCloseButton = document.querySelector("#detail-close");
const detailPrevButton = document.querySelector("#detail-prev");
const detailNextButton = document.querySelector("#detail-next");
const settingsModal = document.querySelector("#settings-modal");
const settingsBackdrop = document.querySelector("#settings-backdrop");
const settingsCloseButton = document.querySelector("#settings-close");
const detailKicker = document.querySelector("#detail-kicker");
const detailContextLine = document.querySelector("#detail-context-line");
const detailTitle = document.querySelector("#detail-title");
const detailTitleLink = document.querySelector("#detail-title-link");
const detailFit = document.querySelector("#detail-fit");
const detailPositionBadge = document.querySelector("#detail-position-badge");
const detailPositionSummary = document.querySelector("#detail-position-summary");
const detailPositionExact = document.querySelector("#detail-position-exact dd");
const detailPositionNearby = document.querySelector("#detail-position-nearby dd");
const detailPositionRegion = document.querySelector("#detail-position-region dd");
const detailWarmthBadge = document.querySelector("#detail-warmth-badge");
const detailWarmthSummary = document.querySelector("#detail-warmth-summary");
const detailWarmthClient = document.querySelector("#detail-warmth-client dd");
const detailWarmthGroup = document.querySelector("#detail-warmth-group dd");
const detailSource = document.querySelector("#detail-source");
const detailOrganization = document.querySelector("#detail-organization");
const detailCountries = document.querySelector("#detail-countries");
const detailDeadline = document.querySelector("#detail-deadline");
const detailType = document.querySelector("#detail-type");
const detailStatus = document.querySelector("#detail-status");
const detailAdded = document.querySelector("#detail-added");
const detailSourceMeta = document.querySelector("#detail-source-meta");
const detailNotes = document.querySelector("#detail-notes");
const detailTargetState = document.querySelector("#detail-target-state");
const detailTargetHelp = document.querySelector("#detail-target-help");
const detailMissedReason = document.querySelector("#detail-missed-reason");
const detailMissedField = document.querySelector("#detail-missed-field");
const detailLink = document.querySelector("#detail-link");
const detailActionHelp = document.querySelector("#detail-action-help");
const detailActionSave = document.querySelector("#detail-action-save");
const detailStateButtons = Array.from(document.querySelectorAll("[data-detail-target-state]"));
const tablePagination = document.querySelector("#table-pagination");
const paginationSummary = document.querySelector("#pagination-summary");
const paginationPage = document.querySelector("#pagination-page");
const paginationPrevButton = document.querySelector("#pagination-prev");
const paginationNextButton = document.querySelector("#pagination-next");
const tenderSearchInput = document.querySelector("#tender-search");
const tenderSearchClearButton = document.querySelector("#tender-search-clear");
const fitFilter = document.querySelector("#filter-fit");
const warmthFilter = document.querySelector("#filter-warmth");
const opportunityFilter = document.querySelector("#filter-opportunity");
const sourceFilter = document.querySelector("#filter-source");
const addedFilter = document.querySelector("#filter-added");
const deadlineFilter = document.querySelector("#filter-deadline");
const statusFilter = document.querySelector("#filter-status");
const linkFilter = document.querySelector("#filter-link");
const fitFilterChip = document.querySelector("#fit-filter-chip");
const warmthFilterChip = document.querySelector("#warmth-filter-chip");
const deadlineFilterChip = document.querySelector("#deadline-filter-chip");
const sourceFilterChip = document.querySelector("#source-filter-chip");
const statusFilterChip = document.querySelector("#status-filter-chip");
const tableSortButtons = Array.from(document.querySelectorAll("[data-sort-column]"));
const viewTableButton = document.querySelector("#view-table");
const viewCardsButton = document.querySelector("#view-cards");
const themeLightButton = document.querySelector("#theme-light");
const themeDarkButton = document.querySelector("#theme-dark");
const themeToggleButton = document.querySelector("#theme-toggle");
const toast = document.querySelector("#toast");
const DISPLAY_SOURCES = ["ReliefWeb", "UNDP Procurement", "UNGM", "ICIMOD", "Welthungerhilfe"];
const STORAGE_KEYS = {
  theme: "fairpicture-opportunities-theme",
  view: "fairpicture-opportunities-view",
};
const DEBUG_STORAGE_KEY = "fairpicture-opportunities-debug";
const AUTH_BYPASS_STORAGE_KEY = "fairpicture-opportunities-auth-bypass";
const ADMIN_AUTH_TOKEN_KEY = "fairpicture-admin-auth-token";
const ADMIN_AUTH_USER_KEY = "fairpicture-admin-auth-user";
const TAB_QUERY_MAP = new Map([
  ["results", "results"],
  ["pending", "pending"],
  ["applied", "applied"],
  ["missed", "missed"],
]);
let allOpportunities = [];
let resultsStatsSnapshot = []; // frozen snapshot from "results" bucket — drives stat cards regardless of active tab
let currentTablePage = 1;
let currentOpportunityTab = "results";
let selectedOpportunityId = null;
let latestSortedOpportunities = [];
let activeBucketRequestId = 0;
let notificationSettings = null;
let notificationRecipientChips = [];
let notificationSettingsBusy = false;
let toastTimeoutId = 0;
let authSession = null;
let authUserProfile = null;
let supabaseClient = null;
let authFlowMode = "sign-in";
let authProvider = "supabase";
let authBypassMode = false;
let tableSortState = { column: "fit", direction: "desc" };
let detailActionBusy = false;
const bucketCounts = {
  results: 0,
  applied: 0,
  missed: 0,
};
const TABLE_PAGE_SIZE = 10;

authForm.addEventListener("submit", handleAuthSubmit);
authEmailInput.addEventListener("input", handleAuthEmailInput);
signOutButton.addEventListener("click", handleSignOut);
fetchButton.addEventListener("click", handleFetch);
openSettingsButton.addEventListener("click", openSettingsModal);
settingsSummaryButton.addEventListener("click", openSettingsModal);
sourcesButton.addEventListener("click", openSourcesDrawer);
sourcesCloseButton.addEventListener("click", closeSourcesDrawer);
sourcesDrawerBackdrop.addEventListener("click", closeSourcesDrawer);
notificationSaveButton.addEventListener("click", handleSaveNotificationSettings);
notificationTestButton.addEventListener("click", handleTestNotification);
resultsTab.addEventListener("click", () => setActiveTab("results"));
pendingTab.addEventListener("click", () => setActiveTab("pending"));
appliedTab.addEventListener("click", () => setActiveTab("applied"));
missedTab.addEventListener("click", () => setActiveTab("missed"));
resultsBody.addEventListener("click", handleOpportunityListClick);
cardsView.addEventListener("click", handleOpportunityListClick);
detailBackdrop.addEventListener("click", closeDetailModal);
detailCloseButton.addEventListener("click", closeDetailModal);
detailPrevButton.addEventListener("click", () => openAdjacentDetailModal(-1));
detailNextButton.addEventListener("click", () => openAdjacentDetailModal(1));
settingsBackdrop.addEventListener("click", closeSettingsModal);
settingsCloseButton.addEventListener("click", closeSettingsModal);
detailActionSave.addEventListener("click", handleDetailActionSubmit);
detailTargetState.addEventListener("change", syncDetailTargetStateUi);
detailMissedReason.addEventListener("change", updateDetailActionButtonState);
detailNotes.addEventListener("input", updateDetailActionButtonState);
detailStateButtons.forEach((button) => {
  button.addEventListener("click", () => {
    if (button.disabled) {
      return;
    }
    detailTargetState.value = button.dataset.detailTargetState || "live";
    syncDetailTargetStateUi();
  });
});
viewTableButton.addEventListener("click", () => setViewMode("table"));
viewCardsButton.addEventListener("click", () => setViewMode("cards"));
themeLightButton.addEventListener("click", () => setTheme("light"));
themeDarkButton.addEventListener("click", () => setTheme("dark"));
if (themeToggleButton) {
  themeToggleButton.addEventListener("click", () => {
    const current = document.documentElement.dataset.theme || "dark";
    setTheme(current === "dark" ? "light" : "dark");
  });
}
[
  fitFilter,
  warmthFilter,
  opportunityFilter,
  sourceFilter,
  addedFilter,
  deadlineFilter,
  statusFilter,
  linkFilter,
].forEach((element) => {
  element.addEventListener(element.tagName === "SELECT" ? "change" : "input", () => {
    currentTablePage = 1;
    applyTableState();
  });
});
tableSortButtons.forEach((button) => {
  button.addEventListener("click", () => {
    updateTableSort(button.dataset.sortColumn || "");
  });
});
paginationPrevButton.addEventListener("click", () => changeTablePage(-1));
paginationNextButton.addEventListener("click", () => changeTablePage(1));
tenderSearchInput.addEventListener("input", handleTenderSearchInput);
tenderSearchClearButton.addEventListener("click", clearTenderSearch);
notificationEmails.addEventListener("click", () => {
  if (!notificationSettingsBusy) {
    notificationEmailInput.focus();
  }
});
notificationEmailInput.addEventListener("keydown", handleNotificationEmailKeydown);
notificationEmailInput.addEventListener("blur", handleNotificationEmailBlur);
notificationEmailChips.addEventListener("click", handleNotificationChipClick);
window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !settingsModal.hidden) {
    closeSettingsModal();
  } else if (event.key === "Escape" && !sourcesDrawer.hidden) {
    closeSourcesDrawer();
  } else if (event.key === "Escape" && !detailModal.hidden) {
    closeDetailModal();
  }
});
window.addEventListener("popstate", () => {
  setActiveTab(getTabFromUrl(), { updateUrl: false });
});
window.addEventListener("load", () => {
  initializePreferences();
  initializeDebugMode();
  initCustomDropdowns();
  setActiveTab(getTabFromUrl(), { updateUrl: false, silent: true });
  bootstrapAuth();
});

function initCustomDropdowns() {
  document.querySelectorAll(".filter-chip__panel").forEach((panel) => {
    const select = panel.querySelector("select");
    if (!select) return;

    const ul = document.createElement("ul");
    ul.className = "filter-chip__options";
    ul.setAttribute("role", "listbox");

    const rebuildOptions = () => {
      ul.innerHTML = "";
      Array.from(select.options).forEach((option) => {
        const li = document.createElement("li");
        li.className = "filter-chip__option";
        if (option.value === select.value) {
          li.classList.add("filter-chip__option--selected");
        }
        li.setAttribute("role", "option");
        li.setAttribute("data-value", option.value);
        li.innerHTML = `<span class="filter-chip__option-check"><svg viewBox="0 0 12 12" fill="none"><path d="M2 6l3 3 5-5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg></span><span>${option.textContent}</span>`;
        li.addEventListener("click", () => {
          select.value = option.value;
          ul.querySelectorAll(".filter-chip__option").forEach((item) => {
            item.classList.toggle("filter-chip__option--selected", item.dataset.value === option.value);
          });
          select.dispatchEvent(new Event("change", { bubbles: true }));
          const details = panel.closest("details");
          if (details) details.removeAttribute("open");
        });
        ul.appendChild(li);
      });
    };

    rebuildOptions();
    panel.appendChild(ul);

    // Re-sync checkmarks if the select value changes externally (e.g. source list populated dynamically)
    select.addEventListener("change", () => {
      ul.querySelectorAll(".filter-chip__option").forEach((item) => {
        item.classList.toggle("filter-chip__option--selected", item.dataset.value === select.value);
      });
    });

    // Watch for new <option> elements added dynamically (source filter is populated at runtime)
    const observer = new MutationObserver(() => rebuildOptions());
    observer.observe(select, { childList: true });
  });

  // Close any open dropdown when clicking outside
  document.addEventListener("click", (e) => {
    if (!e.target.closest(".filter-chip")) {
      document.querySelectorAll(".filter-chip[open]").forEach((d) => d.removeAttribute("open"));
    }
  });
}

async function bootstrapAuth() {
  setBootState(true, "Restoring secure session and preparing the cached workspace.");

  if (shouldEnableAuthBypass()) {
    await applyBypassSession();
    return;
  }

  if (!window.supabase) {
    setAuthMessage("Supabase client failed to load.", true);
    setAuthBusy(true);
    setBootState(false);
    authShell.hidden = false;
    return;
  }

  try {
    const authConfig = await fetchAuthConfig();
    if (!authConfig?.supabaseUrl || !authConfig?.supabaseAnonKey) {
      setAuthMessage(
        "Supabase auth is not configured. You can still use the fallback admin login if it is enabled.",
        true
      );
      setAuthBusy(false);
      setBootState(false);
      authShell.hidden = false;
      return;
    }

    supabaseClient = window.supabase.createClient(authConfig.supabaseUrl, authConfig.supabaseAnonKey);
    setBootState(true, "Checking callback tokens and previous sessions.");
    await handleAuthCallback();

    const {
      data: { session },
    } = await supabaseClient.auth.getSession();
    if (session?.access_token) {
      await applyAuthSession(session);
      return;
    }

    if (restoreAdminSession()) {
      await applyAdminSession();
      return;
    }

    await applyAuthSession(null);

    supabaseClient.auth.onAuthStateChange((_event, sessionState) => {
      void applyAuthSession(sessionState);
    });
  } catch (error) {
    setAuthMessage(error.message || "Could not initialize sign-in.", true);
    setAuthBusy(false);
    setBootState(false);
    authShell.hidden = false;
  }
}

function shouldEnableAuthBypass() {
  if (!isLocalDevHost()) {
    window.localStorage.removeItem(AUTH_BYPASS_STORAGE_KEY);
    return false;
  }

  try {
    const params = new URLSearchParams(window.location.search);
    const requested = params.get("skipAuth");
    if (requested === "1" || requested === "true") {
      window.localStorage.setItem(AUTH_BYPASS_STORAGE_KEY, "1");
      return true;
    }
  } catch (error) {
    // Ignore malformed URL state.
  }

  return window.localStorage.getItem(AUTH_BYPASS_STORAGE_KEY) === "1";
}

function isLocalDevHost() {
  const host = window.location.hostname;
  return host === "localhost" || host === "127.0.0.1" || host === "::1";
}

function clearAuthBypass() {
  authBypassMode = false;
  window.localStorage.removeItem(AUTH_BYPASS_STORAGE_KEY);
}

async function applyBypassSession() {
  authBypassMode = true;
  authSession = { access_token: "dev-auth-bypass" };
  authUserProfile = { email: "local-preview@fairpicture.dev" };
  authProvider = "bypass";

  setBootState(false);
  authShell.hidden = true;
  appShell.hidden = false;
  authUser.hidden = false;
  authUserEmail.textContent = authUserProfile.email;

  allOpportunities = [];
  resultsStatsSnapshot = [];
  bucketCounts.results = 0;
  bucketCounts.applied = 0;
  bucketCounts.missed = 0;
  applyTableState();
  updateSyncMeta(null);
  setStatus("Preview", "success");
  setMessage("Local preview mode is enabled. Sign in to use live API data and actions.", "info");
}

async function fetchAuthConfig() {
  const response = await fetch("/api/auth-config", { cache: "no-store" });
  const payload = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(payload?.error || "Could not load auth configuration.");
  }

  return payload || {};
}

async function handleAuthCallback() {
  const params = parseAuthHash(window.location.hash);
  if (!params.access_token || !params.refresh_token) {
    setAuthFlowMode("sign-in");
    return;
  }

  const { data, error } = await supabaseClient.auth.setSession({
    access_token: params.access_token,
    refresh_token: params.refresh_token,
  });

  clearAuthHash();

  if (error) {
    setAuthFlowMode("sign-in");
    setAuthMessage(error.message || "Could not complete the authentication link.", true);
    return;
  }

  if (params.type === "recovery" || params.type === "invite") {
    authSession = data.session || null;
    authUserProfile = data.user || data.session?.user || null;
    setAuthFlowMode("password-reset");
    authEmailInput.value = authUserProfile?.email || "";
    setAuthMessage("Set your password to finish activating this account.", false);
    setAuthBusy(false);
    return;
  }

  setAuthFlowMode("sign-in");
}

function parseAuthHash(hashValue) {
  const rawHash = String(hashValue || "").replace(/^#/, "");
  return Object.fromEntries(new URLSearchParams(rawHash).entries());
}

function clearAuthHash() {
  try {
    const url = new URL(window.location.href);
    url.hash = "";
    window.history.replaceState({}, "", url);
  } catch (error) {
    window.location.hash = "";
  }
}

function setAuthFlowMode(mode) {
  authFlowMode = mode === "password-reset" ? "password-reset" : "sign-in";
  const isPasswordReset = authFlowMode === "password-reset";

  if (authTitle) {
    authTitle.textContent = isPasswordReset ? "Create password" : "Welcome back";
  }
  authSummary.textContent = isPasswordReset
    ? "Create a password for your invited account to finish access setup."
    : "Sign in with your invited team account.";
  authEmailField.hidden = isPasswordReset;
  authPasswordLabel.textContent = isPasswordReset ? "Create password" : "Password";
  authPasswordInput.placeholder = "";
  authPasswordInput.autocomplete = isPasswordReset ? "new-password" : "current-password";
  authPasswordConfirmField.hidden = !isPasswordReset;
  authPasswordConfirmInput.required = isPasswordReset;
  authSubmitButton.textContent = isPasswordReset ? "Set password" : "Sign in to Radar";
}

function setBootState(isVisible, message = "") {
  if (!bootShell) {
    return;
  }
  bootShell.hidden = !isVisible;
  if (message && bootMessage) {
    bootMessage.textContent = message;
  }
}

function handleAuthEmailInput() {
  if (authFlowMode !== "sign-in") {
    setAuthFlowMode("sign-in");
    authPasswordConfirmInput.value = "";
    setAuthMessage("Sign in with your admin or invited team account.", false);
  }
}

async function applyAuthSession(session) {
  authBypassMode = false;
  authSession = session || null;
  authUserProfile = session?.user || null;
  authProvider = "supabase";
  const isSignedIn = Boolean(authSession?.access_token);

  setBootState(false);
  authShell.hidden = isSignedIn;
  appShell.hidden = !isSignedIn;
  authUser.hidden = !isSignedIn;
  authUserEmail.textContent = authUserProfile?.email || "";

  if (!isSignedIn) {
    allOpportunities = [];
    clearAdminSession();
    authPasswordInput.value = "";
    closeSettingsModal();
    closeDetailModal();
    setAuthBusy(false);
    setAuthFlowMode("sign-in");
    setAuthMessage("Access is limited to invited team members.", false);
    return;
  }

  authPasswordInput.value = "";
  authPasswordConfirmInput.value = "";
  setAuthMessage("", false);
  await loadInitialData();
}

async function applyAdminSession() {
  authBypassMode = false;
  const adminToken = window.localStorage.getItem(ADMIN_AUTH_TOKEN_KEY) || "";
  const adminUser = parseStoredAdminUser(window.localStorage.getItem(ADMIN_AUTH_USER_KEY));

  authSession = { access_token: adminToken };
  authUserProfile = adminUser ? { email: adminUser.email } : null;
  authProvider = "admin";

  setBootState(false);
  authShell.hidden = false;
  appShell.hidden = false;
  authUser.hidden = false;
  authUserEmail.textContent = adminUser?.email || "";
  authShell.hidden = true;
  setAuthMessage("", false);
  await loadInitialData();
}

function restoreAdminSession() {
  const token = window.localStorage.getItem(ADMIN_AUTH_TOKEN_KEY) || "";
  const user = parseStoredAdminUser(window.localStorage.getItem(ADMIN_AUTH_USER_KEY));
  return Boolean(token && user?.email);
}

function storeAdminSession(result) {
  const token = String(result?.token || "");
  const email = String(result?.user?.email || "");
  if (!token || !email) {
    return;
  }
  window.localStorage.setItem(ADMIN_AUTH_TOKEN_KEY, token);
  window.localStorage.setItem(ADMIN_AUTH_USER_KEY, JSON.stringify({ email }));
}

function clearAdminSession() {
  window.localStorage.removeItem(ADMIN_AUTH_TOKEN_KEY);
  window.localStorage.removeItem(ADMIN_AUTH_USER_KEY);
}

function parseStoredAdminUser(rawValue) {
  try {
    const parsed = JSON.parse(rawValue || "{}");
    return parsed && parsed.email ? parsed : null;
  } catch (error) {
    return null;
  }
}

async function handleAuthSubmit(event) {
  event.preventDefault();

  if (authFlowMode === "password-reset") {
    if (!supabaseClient) {
      setAuthMessage("Supabase auth is not configured for password recovery.", true);
      return;
    }
    await handlePasswordSetup();
    return;
  }

  setAuthBusy(true);
  setAuthMessage("Signing in...", false);

  const email = authEmailInput.value.trim();
  const password = authPasswordInput.value;
  const adminResult = await signInWithAdmin(email, password);
  if (adminResult) {
    storeAdminSession(adminResult);
    setAuthBusy(false);
    await applyAdminSession();
    return;
  }

  if (!supabaseClient) {
    setAuthBusy(false);
    setAuthMessage("Supabase auth is not configured, and the fallback admin login was rejected.", true);
    return;
  }

  const { error } = await supabaseClient.auth.signInWithPassword({ email, password });

  if (error) {
    setAuthBusy(false);
    setAuthMessage(error.message || "Sign-in failed.", true);
    return;
  }

  setAuthBusy(false);
}

async function handlePasswordSetup() {
  if (!authSession?.access_token) {
    setAuthFlowMode("sign-in");
    setAuthMessage("This password setup link has expired. Request a new one.", true);
    return;
  }

  const password = authPasswordInput.value;
  const passwordConfirm = authPasswordConfirmInput.value;

  if (!password || password.length < 8) {
    setAuthMessage("Use a password with at least 8 characters.", true);
    return;
  }

  if (password !== passwordConfirm) {
    setAuthMessage("The password confirmation does not match.", true);
    return;
  }

  setAuthBusy(true);
  setAuthMessage("Saving password...", false);

  const { error } = await supabaseClient.auth.updateUser({ password });
  if (error) {
    setAuthBusy(false);
    setAuthMessage(error.message || "Could not set the password.", true);
    return;
  }

  authPasswordConfirmInput.value = "";
  setAuthBusy(false);
  setAuthMessage("", false);
}

async function handleSignOut() {
  if (authProvider === "bypass") {
    clearAuthBypass();
    authSession = null;
    authUserProfile = null;
    authProvider = "supabase";
    await applyAuthSession(null);
    return;
  }

  clearAdminSession();

  if (authProvider === "admin") {
    authSession = null;
    authUserProfile = null;
    authProvider = "supabase";
    await applyAuthSession(null);
    return;
  }

  if (supabaseClient) {
    await supabaseClient.auth.signOut();
  }
  closeSettingsModal();
  closeDetailModal();
}

async function signInWithAdmin(email, password) {
  const response = await fetch("/api/admin-login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ email, password }),
  });

  const result = await response.json().catch(() => null);
  if (response.ok) {
    return result;
  }

  return null;
}

function setAuthBusy(isBusy) {
  authEmailInput.disabled = isBusy;
  authPasswordInput.disabled = isBusy;
  authPasswordConfirmInput.disabled = isBusy;
  authSubmitButton.disabled = isBusy;
}

function setAuthMessage(message, isError) {
  authMessage.textContent = message;
  authMessage.style.color = isError ? "#8d3326" : "";
}

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

function getTabFromUrl() {
  try {
    const params = new URLSearchParams(window.location.search);
    const requested = params.get("tab");
    return TAB_QUERY_MAP.has(requested) ? requested : "results";
  } catch (error) {
    return "results";
  }
}

function syncTabUrl(tab) {
  try {
    const url = new URL(window.location.href);
    if (tab === "results") {
      url.searchParams.delete("tab");
    } else {
      url.searchParams.set("tab", tab);
    }
    window.history.pushState({}, "", url);
  } catch (error) {
    debugLog("syncTabUrl:failed", { error });
  }
}

function getColumnFilters() {
  return {
    search: tenderSearchInput.value.trim(),
    fit: fitFilter.value,
    warmth: warmthFilter.value,
    opportunity: opportunityFilter.value,
    source: sourceFilter.value,
    added: addedFilter.value,
    deadline: deadlineFilter.value,
    status: statusFilter.value,
    link: linkFilter.value,
  };
}

function updateTableSort(column) {
  if (!column) {
    return;
  }

  if (tableSortState.column === column) {
    tableSortState = {
      column,
      direction: tableSortState.direction === "asc" ? "desc" : "asc",
    };
  } else {
    tableSortState = {
      column,
      direction: getDefaultSortDirection(column),
    };
  }

  currentTablePage = 1;
  updateTableSortUi();
  applyTableState();
}

function getDefaultSortDirection(column) {
  return ["fit", "warmth", "added", "deadline"].includes(column) ? "desc" : "asc";
}

function updateTableSortUi() {
  tableSortButtons.forEach((button) => {
    const isActive = button.dataset.sortColumn === tableSortState.column;
    button.classList.toggle("active", isActive);
    button.dataset.direction = isActive ? tableSortState.direction : "";
    button.setAttribute(
      "aria-label",
      isActive
        ? `Sorted by ${button.textContent.trim()} ${tableSortState.direction === "asc" ? "ascending" : "descending"}`
        : `Sort by ${button.textContent.trim()}`
    );
    const th = button.closest("th");
    if (th) {
      th.setAttribute(
        "aria-sort",
        isActive ? (tableSortState.direction === "asc" ? "ascending" : "descending") : "none"
      );
    }
  });
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || "").trim());
}

function normalizeRecipientChip(value) {
  const email = String(value || "").trim();
  if (!email) {
    return null;
  }
  return {
    value: email,
    valid: isValidEmail(email),
  };
}

function setRecipientChips(values) {
  notificationRecipientChips = [];
  (Array.isArray(values) ? values : []).forEach((value) => {
    const chip = normalizeRecipientChip(value);
    if (!chip) {
      return;
    }
    if (notificationRecipientChips.some((entry) => entry.value.toLowerCase() === chip.value.toLowerCase())) {
      return;
    }
    notificationRecipientChips.push(chip);
  });
  renderRecipientChips();
}

function addRecipientChip(value) {
  const chip = normalizeRecipientChip(value);
  if (!chip) {
    return;
  }
  if (notificationRecipientChips.some((entry) => entry.value.toLowerCase() === chip.value.toLowerCase())) {
    notificationEmailInput.value = "";
    renderRecipientChips();
    return;
  }
  notificationRecipientChips.push(chip);
  notificationEmailInput.value = "";
  renderRecipientChips();
}

function renderRecipientChips() {
  notificationEmailChips.innerHTML = notificationRecipientChips
    .map((chip, index) => `
      <span class="chip${chip.valid ? "" : " chip--invalid"}" title="${chip.valid ? chip.value : `${chip.value} is not a valid email`}">
        <span>${escapeHtml(chip.value)}</span>
        <button
          class="chip__remove"
          type="button"
          data-chip-index="${index}"
          aria-label="Remove ${escapeAttribute(chip.value)}"
          ${notificationSettingsBusy ? "disabled" : ""}
        >
          ×
        </button>
      </span>
    `)
    .join("");

  const invalidCount = notificationRecipientChips.filter((chip) => !chip.valid).length;
  notificationEmailError.hidden = invalidCount === 0;
  notificationEmailError.textContent = invalidCount
    ? `${invalidCount} recipient ${invalidCount === 1 ? "address needs" : "addresses need"} a valid email format.`
    : "";
}

function handleNotificationEmailKeydown(event) {
  if (notificationSettingsBusy) {
    return;
  }

  if (event.key === "Enter" || event.key === ",") {
    event.preventDefault();
    addRecipientChip(notificationEmailInput.value);
    return;
  }

  if (event.key === "Backspace" && !notificationEmailInput.value && notificationRecipientChips.length > 0) {
    notificationRecipientChips.pop();
    renderRecipientChips();
  }
}

function handleNotificationEmailBlur() {
  if (!notificationSettingsBusy && notificationEmailInput.value.trim()) {
    addRecipientChip(notificationEmailInput.value);
  }
}

function handleNotificationChipClick(event) {
  const button = event.target.closest("[data-chip-index]");
  if (!button || notificationSettingsBusy) {
    return;
  }
  const index = Number(button.getAttribute("data-chip-index"));
  if (!Number.isInteger(index)) {
    return;
  }
  notificationRecipientChips.splice(index, 1);
  renderRecipientChips();
  notificationEmailInput.focus();
}

async function handleFetch() {
  fetchButton.disabled = true;
  setStatus("Syncing...", "loading");
  setMessage("Checking live sources and saving fresh opportunities into the database.", "info");

  try {
    const refreshResponse = await refreshOpportunities();
    await reloadOpportunitiesFromApi({ preserveMessage: true });
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

async function getAccessToken() {
  if (authProvider === "bypass" || authBypassMode) {
    throw new Error("Local preview mode is active. Sign in to access API data.");
  }

  if (authSession?.access_token) {
    return authSession.access_token;
  }

  const adminToken = window.localStorage.getItem(ADMIN_AUTH_TOKEN_KEY) || "";
  if (adminToken) {
    authSession = { access_token: adminToken };
    authProvider = "admin";
    return adminToken;
  }

  if (!supabaseClient) {
    throw new Error("Supabase auth is not configured.");
  }

  const {
    data: { session },
  } = await supabaseClient.auth.getSession();
  authSession = session || null;
  authProvider = "supabase";
  if (!authSession?.access_token) {
    throw new Error("Your session expired. Sign in again.");
  }
  return authSession.access_token;
}

async function fetchWithAuth(input, init = {}) {
  const token = await getAccessToken();
  const headers = new Headers(init.headers || {});
  headers.set("Authorization", `Bearer ${token}`);

  const response = await fetch(input, {
    ...init,
    headers,
  });

  if (response.status === 401) {
    if (authProvider === "admin") {
      clearAdminSession();
      authSession = null;
      authUserProfile = null;
      authProvider = "supabase";
    } else if (supabaseClient) {
      await supabaseClient.auth.signOut();
    }
    throw new Error("Your access is no longer valid for this workspace.");
  }

  return response;
}

async function fetchOpportunities(options = {}) {
  const bucket = options.bucket || getApiBucketForTab(currentOpportunityTab);
  const url = new URL("/api/opportunities", window.location.origin);
  url.searchParams.set("bucket", bucket);
  if (options.noStore) {
    url.searchParams.set("ts", String(Date.now()));
  }
  const response = await fetchWithAuth(
    url.toString(),
    options.noStore ? { cache: "no-store" } : undefined
  );
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
  const response = await fetchWithAuth(url.toString(), { cache: "no-store" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not load sync status.");
  }

  return responseData?.sync || null;
}

async function fetchNotificationSettings() {
  const url = new URL("/api/notification-settings", window.location.origin);
  url.searchParams.set("ts", String(Date.now()));
  const response = await fetchWithAuth(url.toString(), { cache: "no-store" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not load notification settings.");
  }

  return responseData?.settings || null;
}

async function saveNotificationSettings(settings) {
  const response = await fetchWithAuth("/api/notification-settings", {
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
  const response = await fetchWithAuth("/api/test-notification", { method: "POST" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "Could not send the test notification.");
  }

  return responseData?.result || null;
}

async function refreshOpportunities() {
  const response = await fetchWithAuth("/api/refresh", { method: "POST" });
  const responseData = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(responseData?.error || "The refresh request failed.");
  }

  return responseData || {};
}

async function saveOpportunityAction(id, targetState, notes = "", actionReason = "") {
  const response = await fetchWithAuth("/api/opportunity-action", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      id,
      targetState,
      actionReason,
      notes,
    }),
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
  if (bucket === "results") {
    resultsStatsSnapshot = opportunities;
  }
  bucketCounts[currentOpportunityTab] = opportunities.length;
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
  if (bucket === "applied") {
    return count
      ? "Showing applied opportunities from the cached database."
      : "No applied opportunities yet.";
  }

  if (bucket === "pending") {
    return count
      ? "Showing pending opportunities that are in progress but not submitted."
      : "No pending opportunities yet.";
  }

  if (bucket === "missed") {
    return count
      ? "Showing missed, expired, and archived opportunities."
      : "No missed or archived opportunities yet.";
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
  resultsFilterSummary.textContent = "0 results";
  renderSourceHealth([]);
  renderTable([]);
  renderCards([]);
  updatePagination(0, 0, 1);
}

function applyTableState() {
  populateColumnFilterOptions(allOpportunities);
  const filters = getColumnFilters();
  const filtered = filterOpportunities(allOpportunities, filters);
  const sorted = sortOpportunities(filtered, tableSortState);
  latestSortedOpportunities = sorted;
  const totalPages = Math.max(1, Math.ceil(sorted.length / TABLE_PAGE_SIZE));
  currentTablePage = Math.min(currentTablePage, totalPages);
  const paginated = paginateOpportunities(sorted, currentTablePage, TABLE_PAGE_SIZE);
  updateResultsHeader(sorted.length);
  updateFilterSummary(sorted.length, allOpportunities.length, filters, tableSortState);
  updateDashboardStats(resultsStatsSnapshot.length > 0 ? resultsStatsSnapshot : allOpportunities);
  updateTabCounts();
  updateFilterChipUi(filters);
  renderSourceHealth(getSourceCountsForItems(allOpportunities));
  renderResults(sorted, paginated);
  updatePagination(sorted.length, paginated.length, totalPages);
  resultCount.textContent = `${sorted.length} ${sorted.length === 1 ? "item" : "items"}`;
  if (!detailModal.hidden && selectedOpportunityId) {
    syncDetailNavigation();
  }
  debugLog("applyTableState", {
    tab: currentOpportunityTab,
    filters,
    sortBy: tableSortState,
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

function populateColumnFilterOptions(opportunities) {
  const typeOptions = Array.from(
    new Set(
      (Array.isArray(opportunities) ? opportunities : [])
        .map((item) => getOpportunityTypeFilterValue(item))
        .filter(Boolean)
    )
  ).sort((left, right) => safeCompare(left, right));

  const previousValue = opportunityFilter.value || "all";
  opportunityFilter.innerHTML = [
    '<option value="all">All types</option>',
    ...typeOptions.map((value) => `<option value="${escapeAttribute(value)}">${escapeHtml(value)}</option>`),
  ].join("");
  opportunityFilter.value = typeOptions.includes(previousValue) ? previousValue : "all";
}

function updateResultsHeader(count) {
  const config = getResultTabConfig(currentOpportunityTab);
  resultsSectionLabel.textContent = config.label;
  resultsTitle.textContent = config.title;
  resultsSummary.textContent = config.summary(count);
}

function updateFilterSummary(filteredCount, totalCount, filters, sortState) {
  const activeFilterCount = Object.entries(filters || {}).filter(([key, value]) => {
    return value && value !== "all";
  }).length;

  if (activeFilterCount === 0) {
    resultsFilterSummary.textContent = `${totalCount} ${totalCount === 1 ? "result" : "results"}`;
    return;
  }

  const parts = [`Showing ${filteredCount} of ${totalCount} results`];
  parts.push(`${activeFilterCount} active filter${activeFilterCount === 1 ? "" : "s"}`);
  if (sortState?.column) {
    parts.push(`sorted by ${sortState.column} ${sortState.direction}`);
  }
  resultsFilterSummary.textContent = parts.join(" ");
}

function paginateOpportunities(opportunities, page, pageSize) {
  const safePage = Math.max(1, page);
  const startIndex = (safePage - 1) * pageSize;
  return opportunities.slice(startIndex, startIndex + pageSize);
}

function updatePagination(totalCount, pageCount, totalPages) {
  const hasPagination = totalCount > TABLE_PAGE_SIZE;
  tablePagination.hidden = !hasPagination;

  const start = totalCount === 0 ? 0 : (currentTablePage - 1) * TABLE_PAGE_SIZE + 1;
  const end = totalCount === 0 ? 0 : start + pageCount - 1;
  toolbarRange.textContent = `${start}-${end} of ${totalCount}`;

  if (!hasPagination) {
    return;
  }

  paginationSummary.textContent = `Showing ${start}-${end} of ${totalCount}`;
  paginationPage.textContent = `Page ${currentTablePage} of ${totalPages}`;
  paginationPrevButton.disabled = currentTablePage <= 1;
  paginationNextButton.disabled = currentTablePage >= totalPages;
}

function updateDashboardStats(items) {
  const list = Array.isArray(items) ? items : [];
  const highFitCount = list.filter((item) => getFitFilterValue(item) === "high").length;
  const expiringSoonCount = list.filter((item) => {
    const days = getDaysUntilDeadline(item.deadline);
    return days !== null && days >= 0 && days <= 7;
  }).length;
  const appliedCount = list.filter((item) => item.actionStatus === "applied").length;
  const recentHighFitCount = list.filter((item) => getFitFilterValue(item) === "high" && matchesAddedFilter(item.addedAt, "last-7-days")).length;

  statTotal.textContent = String(list.length);
  statHighFit.textContent = String(highFitCount);
  statHighFitSub.textContent = `↑ ${recentHighFitCount} new this week`;
  statExpiring.textContent = String(expiringSoonCount);
  statApplied.textContent = String(appliedCount);
}

function updateTabCounts() {
  resultsTabCount.textContent = String(bucketCounts.results || 0);
  appliedTabCount.textContent = String(bucketCounts.applied || 0);
  missedTabCount.textContent = String(bucketCounts.missed || 0);
}

function updateFilterChipUi(filters) {
  updateFilterChipLabel(fitFilterChip, "Fit", fitFilter, filters.fit);
  updateFilterChipLabel(warmthFilterChip, "Client", warmthFilter, filters.warmth);
  updateFilterChipLabel(deadlineFilterChip, "Deadline", deadlineFilter, filters.deadline);
  updateFilterChipLabel(sourceFilterChip, "Source", sourceFilter, filters.source);
  updateFilterChipLabel(statusFilterChip, "Status", statusFilter, filters.status);
}

function updateFilterChipLabel(node, label, selectNode, value) {
  if (!node || !selectNode) {
    return;
  }
  const selectedOption = selectNode.options[selectNode.selectedIndex];
  const active = value && value !== "all";
  node.parentElement.classList.toggle("filter-chip--active", active);
  node.childNodes[0].textContent = active && selectedOption ? `${label}: ${selectedOption.textContent}` : label + " ";
}

function changeTablePage(delta) {
  currentTablePage = Math.max(1, currentTablePage + delta);
  applyTableState();
}

function filterOpportunities(opportunities, filters) {
  return opportunities.filter((opportunity) => {
    if (filters.search && !matchesTenderSearch(opportunity, filters.search)) {
      return false;
    }

    const sourceList = Array.isArray(opportunity.sourceList) && opportunity.sourceList.length > 0
      ? opportunity.sourceList
      : [opportunity.source];

    if (filters.source !== "all" && !sourceList.includes(filters.source)) {
      return false;
    }

    if (filters.fit !== "all" && getFitFilterValue(opportunity) !== filters.fit) {
      return false;
    }

    if (filters.warmth !== "all" && !matchesWarmthFilter(opportunity, filters.warmth)) {
      return false;
    }

    if (filters.opportunity !== "all" && getOpportunityTypeFilterValue(opportunity) !== filters.opportunity) {
      return false;
    }

    if (filters.added !== "all" && !matchesAddedFilter(opportunity.addedAt, filters.added)) {
      return false;
    }

    if (filters.deadline !== "all" && !matchesDeadlineFilter(opportunity.deadline, filters.deadline)) {
      return false;
    }

    if (filters.status !== "all") {
      const statusValue = getStatusFilterValue(opportunity);
      if (statusValue !== filters.status) {
        return false;
      }
    }

    if (filters.link === "has-link" && !opportunity.link) {
      return false;
    }

    if (filters.link === "no-link" && opportunity.link) {
      return false;
    }

    return true;
  });
}

function getFitFilterValue(opportunity) {
  const fitScore = Number(opportunity.fitScore) || 0;
  if (fitScore >= 70) {
    return "high";
  }
  if (fitScore >= 40) {
    return "medium";
  }
  return "low";
}

function getOpportunityTypeFilterValue(opportunity) {
  return String(opportunity.type || "Opportunity").trim() || "Opportunity";
}

function matchesAddedFilter(value, mode) {
  const days = getDaysFromNow(value);
  if (days === null) {
    return false;
  }
  if (mode === "last-7-days") {
    return days >= 0 && days <= 7;
  }
  if (mode === "last-30-days") {
    return days >= 0 && days <= 30;
  }
  if (mode === "older") {
    return days > 30;
  }
  return true;
}

function matchesDeadlineFilter(value, mode) {
  const days = getDaysUntilDeadline(value);
  if (mode === "unknown") {
    return days === null;
  }
  if (days === null) {
    return false;
  }
  if (mode === "overdue") {
    return days < 0;
  }
  if (mode === "today") {
    return days === 0;
  }
  if (mode === "next-7-days") {
    return days >= 0 && days <= 7;
  }
  if (mode === "next-30-days") {
    return days >= 0 && days <= 30;
  }
  if (mode === "later") {
    return days > 30;
  }
  return true;
}

function getDaysFromNow(value) {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  const now = new Date();
  const nowStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const dateStart = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  return Math.round((nowStart - dateStart) / 86400000);
}

function sortOpportunities(opportunities, sortState) {
  const items = [...opportunities];
  const column = sortState?.column || "fit";
  const direction = sortState?.direction === "asc" ? 1 : -1;

  items.sort((left, right) => {
    const comparison = getSortComparison(left, right, column);
    return comparison * direction || safeCompare(left.title, right.title);
  });

  return items;
}

function getSortComparison(left, right, column) {
  if (column === "fit") {
    return (Number(left.fitScore) || 0) - (Number(right.fitScore) || 0);
  }

  if (column === "warmth") {
    return getWarmthDescriptor(left).score - getWarmthDescriptor(right).score;
  }

  if (column === "opportunity") {
    return safeCompare(left.title, right.title);
  }

  if (column === "source") {
    return safeCompare(getOpportunitySourceLabel(left), getOpportunitySourceLabel(right));
  }

  if (column === "added") {
    return parseSortableDate(left.addedAt) - parseSortableDate(right.addedAt);
  }

  if (column === "deadline") {
    return parseSortableDate(left.deadline) - parseSortableDate(right.deadline);
  }

  if (column === "status") {
    return safeCompare(getStatusLabel(left), getStatusLabel(right));
  }

  if (column === "link") {
    return safeCompare(left.link ? "1" : "0", right.link ? "1" : "0");
  }

  return 0;
}

function getStatusFilterValue(opportunity) {
  if (isExpiringSoon(opportunity)) {
    return "expiring-soon";
  }
  if (opportunity.actionStatus === "reviewed") {
    return "reviewed";
  }
  if (opportunity.actionStatus === "applied") {
    return "applied";
  }
  if (opportunity.actionStatus === "pending") {
    return "pending";
  }
  if (opportunity.actionStatus === "missed") {
    return "missed";
  }
  if (opportunity.status === "stale") {
    return "archived";
  }
  if (opportunity.status === "expired") {
    return "expired";
  }
  return "live";
}

function renderTable(opportunities) {
  if (opportunities.length === 0) {
    resultsBody.innerHTML =
      '<tr class="placeholder-row"><td colspan="9">No results to display.</td></tr>';
    return;
  }

  resultsBody.innerHTML = opportunities
    .map((opportunity) => {
      const fitTone = getFitTone(Number(opportunity.fitScore) || 0);
      const positionDescriptor = getPositionDescriptor(opportunity);
      const warmthDescriptor = getWarmthDescriptor(opportunity);
      const safeTitle = escapeHtml(opportunity.title);
      const safeSecondary = escapeHtml(getOpportunitySecondaryLine(opportunity));
      const deadlineMeta = getDeadlineMeta(opportunity);
      const safeLink = escapeAttribute(opportunity.link || "");
      const safeSource = escapeHtml(getOpportunitySourceLabel(opportunity));
      const safeAddedAt = escapeHtml(formatCompactDate(opportunity.addedAt));
      const safeStatus = escapeHtml(getStatusBadgeLabel(opportunity));
      const safeId = escapeAttribute(opportunity.id || "");

      return `
        <tr class="clickable-row" data-opportunity-id="${safeId}">
          <td data-label="Fit">
            <span class="fit-badge fit-badge--${fitTone}">${escapeHtml(getFitBadgeLabel(opportunity))}</span>
          </td>
          <td data-label="Client">
            ${renderWarmthBadge(warmthDescriptor)}
          </td>
          <td data-label="Position">
            ${renderPositionBadge(positionDescriptor)}
          </td>
          <td data-label="Opportunity">
            <p class="opportunity-title">${safeTitle}</p>
            <small class="opportunity-subline">${safeSecondary}</small>
          </td>
          <td data-label="Source">
            <span class="source-tag">${safeSource}</span>
          </td>
          <td data-label="Added">${safeAddedAt}</td>
          <td data-label="Deadline"><span class="deadline-cell deadline-cell--${deadlineMeta.tone}">${escapeHtml(deadlineMeta.label)}</span></td>
          <td data-label="Status"><span class="status-badge status-badge--${getStatusTone(opportunity)}">${safeStatus}</span></td>
          <td data-label="Link">
            ${
              safeLink
                ? `<a class="result-link result-link--button" href="${safeLink}" target="_blank" rel="noreferrer" data-stop-row-open="true"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 2H14v4.5h-1.5v-2L7.1 9.9 6 8.8 11.4 3.5h-1.9V2ZM3 4h4v1.5H4.5v6h6V9H12v4H3V4Z"></path></svg><span>Open</span></a>`
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
      const fitTone = getFitTone(Number(opportunity.fitScore) || 0);
      const positionDescriptor = getPositionDescriptor(opportunity);
      const warmthDescriptor = getWarmthDescriptor(opportunity);
      const safeTitle = escapeHtml(opportunity.title);
      const safeOrganization = escapeHtml(opportunity.organization || "N/A");
      const safeCountries = escapeHtml(
        Array.isArray(opportunity.countryList) && opportunity.countryList.length > 0
          ? opportunity.countryList.join(", ")
          : "Global / unspecified"
      );
      const safeDeadline = escapeHtml(getDeadlineMeta(opportunity).label);
      const safeAddedAt = escapeHtml(formatCompactDate(opportunity.addedAt));
      const safeType = escapeHtml(opportunity.type || "Opportunity");
      const safeLink = escapeAttribute(opportunity.link || "");
      const safeSource = escapeHtml(getOpportunitySourceLabel(opportunity));
      const safeStatus = escapeHtml(getStatusBadgeLabel(opportunity));
      const safeId = escapeAttribute(opportunity.id || "");

      return `
        <article class="opportunity-card opportunity-card--${fitTone}${expiringSoon ? " opportunity-card--expiring" : ""}" data-opportunity-id="${safeId}">
          <div class="opportunity-card__top">
            <div class="opportunity-card__source-tags">
              <span class="source-tag">${safeSource}</span>
            </div>
            <div class="opportunity-card__badges">
              <span class="fit-badge fit-badge--${fitTone}">${escapeHtml(getFitBadgeLabel(opportunity))}</span>
              ${warmthDescriptor.tone === "new" ? "" : renderWarmthBadge(warmthDescriptor)}
              ${renderPositionBadge(positionDescriptor)}
            </div>
          </div>

          <div class="opportunity-card__body">
            <h3>${safeTitle}</h3>
            <p class="cell-subtext">${escapeHtml(getOpportunitySecondaryLine(opportunity))}</p>
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
              <dd><span class="source-tag">${safeType}</span></dd>
            </div>
            <div>
              <dt>Status</dt>
              <dd><span class="status-badge status-badge--${getStatusTone(opportunity)}">${safeStatus}</span></dd>
            </div>
          </dl>

          <div class="opportunity-card__footer">
            ${
              safeLink
                ? `<a class="result-link result-link--button" href="${safeLink}" target="_blank" rel="noreferrer" data-stop-row-open="true"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 2H14v4.5h-1.5v-2L7.1 9.9 6 8.8 11.4 3.5h-1.9V2ZM3 4h4v1.5H4.5v6h6V9H12v4H3V4Z"></path></svg><span>Open</span></a>`
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
  if ((currentOpportunityTab === "pending" || currentOpportunityTab === "applied" || currentOpportunityTab === "missed") && opportunity.actionStatus) {
    const takenAt = opportunity.actionTakenAt ? ` on ${formatTimestamp(opportunity.actionTakenAt)}` : "";
    return `${getActionLabel(opportunity.actionStatus, opportunity.missedReason)}${takenAt}`;
  }

  if (opportunity.status === "stale") {
    return "No longer returned by the source, kept here for reference.";
  }

  if (opportunity.status === "expired") {
    return "Deadline passed and the tender moved out of the live queue.";
  }

  return "Keyword match from the job title or procurement notice.";
}

function getOpportunityMetaLine(opportunity) {
  if (currentOpportunityTab === "pending" && opportunity.actionStatus === "pending") {
    return getListSummary(opportunity);
  }

  if (currentOpportunityTab === "applied" && opportunity.actionStatus === "applied") {
    return getListSummary(opportunity);
  }

  if (currentOpportunityTab === "missed") {
    return getMissedReasonLabel(opportunity.missedReason) || getListSummary(opportunity);
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

function getOpportunitySecondaryLine(opportunity) {
  const type = String(opportunity.type || "Opportunity").trim();
  const organization = String(opportunity.organization || "N/A").trim();
  return `${type} · ${organization}`;
}

function getOpportunitySourceLabel(opportunity) {
  const sourceList = Array.isArray(opportunity.sourceList) && opportunity.sourceList.length > 0
    ? opportunity.sourceList
    : [opportunity.source || "Source"];
  if (sourceList.length === 1) {
    return sourceList[0];
  }
  return `${sourceList[0]} +${sourceList.length - 1}`;
}

function getOpportunitySourceDetail(opportunity) {
  const sourceList = Array.isArray(opportunity.sourceList) && opportunity.sourceList.length > 0
    ? opportunity.sourceList
    : [opportunity.source || "Source"];
  return sourceList.join(", ");
}

function getOpportunityCountryList(opportunity) {
  return Array.isArray(opportunity.countryList) && opportunity.countryList.length > 0
    ? opportunity.countryList
    : [];
}

function getOpportunityCountryLabel(opportunity, options = {}) {
  const countries = getOpportunityCountryList(opportunity);
  if (countries.length === 0) {
    return options.short ? "Global" : "Global / unspecified";
  }
  return options.short ? countries[0] : countries.join(", ");
}

function normalizeOpportunityToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
}

function looksLikeOpportunityRawId(value) {
  const candidate = String(value || "").trim();
  return /[0-9]/.test(candidate) && /^[A-Z0-9]+(?:-[A-Z0-9]+)+$/.test(candidate);
}

function getOpportunityRawId(opportunity) {
  const title = sanitizeOpportunityTitle(opportunity.title);
  const [firstSegment = ""] = title.split(/\s+-\s+/);
  if (looksLikeOpportunityRawId(firstSegment)) {
    return firstSegment;
  }
  const [noticeId = ""] = extractNoticeIds(opportunity.link);
  return noticeId || opportunity.id || "—";
}

function getOpportunityDisplayTitle(opportunity) {
  const title = sanitizeOpportunityTitle(opportunity.title);
  const parts = title.split(/\s+-\s+/).map((part) => part.trim()).filter(Boolean);
  if (parts.length === 0) {
    return "Opportunity";
  }

  if (looksLikeOpportunityRawId(parts[0])) {
    parts.shift();
  }

  const countryTokens = new Set(
    getOpportunityCountryList(opportunity)
      .map((country) => normalizeOpportunityToken(country))
      .filter(Boolean)
  );

  if (parts.length > 1 && countryTokens.has(normalizeOpportunityToken(parts[parts.length - 1]))) {
    parts.pop();
  }

  if (
    parts.length > 1 &&
    normalizeOpportunityToken(parts[parts.length - 1]) === normalizeOpportunityToken(opportunity.organization)
  ) {
    parts.pop();
  }

  return parts.join(" - ") || title || "Opportunity";
}

function getDetailTypeLabel(opportunity) {
  const rawType = String(opportunity.type || "").trim();
  if (rawType && rawType.toUpperCase() !== "OTHER") {
    return rawType;
  }

  const title = sanitizeOpportunityTitle(opportunity.title).toLowerCase();
  const contentTypes = [];
  if (/(video|videography|videographer)/.test(title)) {
    contentTypes.push("Videography");
  }
  if (/(photo|photography|photographer)/.test(title)) {
    contentTypes.push("Photography");
  }
  if (/(film|filming)/.test(title) && !contentTypes.includes("Videography")) {
    contentTypes.push("Filming");
  }
  if (/(documentary|storytelling|multimedia)/.test(title)) {
    contentTypes.push("Storytelling");
  }

  const qualifiers = [];
  if (/\blta\b|long term agreement/.test(title)) {
    qualifiers.push("LTA");
  }
  if (/\brfp\b|request for proposal/.test(title)) {
    qualifiers.push("RFP");
  }

  const contentLabel = contentTypes.join(" / ");
  if (qualifiers.length && contentLabel) {
    return `${qualifiers[0]} · ${contentLabel}`;
  }
  if (qualifiers.length) {
    return qualifiers[0];
  }
  if (contentLabel) {
    return contentLabel;
  }
  return "Procurement opportunity";
}

function getDetailDeadlineMeta(value) {
  if (!value) {
    return { label: "No deadline listed", suffix: "", tone: "default" };
  }

  const days = getDaysUntilDeadline(value);
  const label = formatDate(value);
  if (days === null) {
    return { label, suffix: "", tone: "default" };
  }
  if (days <= 1) {
    if (days < 0) {
      return { label, suffix: "Passed", tone: "critical" };
    }
    return { label, suffix: days === 0 ? "Today" : "Tomorrow", tone: "critical" };
  }
  if (days <= 7) {
    return { label, suffix: `${days} days`, tone: "warning" };
  }
  return { label, suffix: "", tone: "default" };
}

function getDetailFitDescriptor(opportunity) {
  const fitScore = Number(opportunity.fitScore) || 0;
  const fitTone = getFitTone(fitScore);
  if (fitTone === "high") {
    return { tone: "high", label: `★ ${fitScore}% High fit` };
  }
  if (fitTone === "medium") {
    return { tone: "medium", label: `${fitScore}% Med fit` };
  }
  return { tone: "low", label: `${fitScore}% Low fit` };
}

function getPositionDescriptor(opportunity) {
  const position = opportunity?.fairpicturePosition || {};
  const tone = ["strong", "good", "emerging", "none"].includes(position.tone)
    ? position.tone
    : "none";
  return {
    label: position.label || "No evidence",
    tone,
    summary: position.summary || "No matching Fairpicture country or regional evidence was found.",
    evidence: position.evidence || {},
  };
}

function renderPositionBadge(descriptor) {
  return `<span class="position-badge position-badge--${escapeAttribute(descriptor.tone)}">${escapeHtml(descriptor.label)}</span>`;
}

function getWarmthDescriptor(opportunity) {
  const warmth = opportunity?.clientWarmth || {};
  const tone = ["client", "family", "network", "new"].includes(warmth.tone) ? warmth.tone : "new";
  return {
    label: warmth.label || "New contact",
    tone,
    score: Number(warmth.score) || 0,
    summary: warmth.summary || "No existing Fairpicture relationship was found for this organisation.",
    evidence: warmth.evidence || {},
  };
}

function renderWarmthBadge(descriptor) {
  // Most tenders come from organisations Fairpicture has never worked with. Rendering a badge
  // on every one of them would bury the handful that are actually warm.
  if (descriptor.tone === "new") {
    return '<span class="warmth-empty" title="No existing Fairpicture relationship">—</span>';
  }
  const suffix = descriptor.tone === "client" ? getWarmthClientSuffix(descriptor) : "";
  return `<span class="warmth-badge warmth-badge--${escapeAttribute(descriptor.tone)}" title="${escapeAttribute(descriptor.summary)}">${escapeHtml(descriptor.label + suffix)}</span>`;
}

function getWarmthClientSuffix(descriptor) {
  const count = Number(descriptor.evidence?.client?.projectCount) || 0;
  return count > 0 ? ` · ${count}` : "";
}

function getWarmthFilterValue(opportunity) {
  return getWarmthDescriptor(opportunity).tone;
}

function matchesWarmthFilter(opportunity, mode) {
  const tone = getWarmthFilterValue(opportunity);
  if (mode === "warm") {
    return tone !== "new";
  }
  return tone === mode;
}

function formatWarmthClient(client) {
  if (!client) {
    return "No direct client match";
  }
  const count = Number(client.projectCount) || 0;
  return `${client.name} — ${count} project${count === 1 ? "" : "s"}`;
}

function formatWarmthGroup(evidence) {
  const group = evidence?.family || evidence?.network;
  if (!group) {
    return "No related organisation or network";
  }
  const names = (group.clients || []).map((client) => client.name).join(", ");
  return names ? `${group.name} — ${names}` : group.name;
}

function formatPositionRecords(records) {
  if (!Array.isArray(records) || records.length === 0) {
    return "No evidence";
  }
  return records
    .map((record) => {
      const country = record.country || "Country";
      const total = Number(record.totalProjects) || 0;
      const projects2026 = Number(record.projects2026) || 0;
      const projects2025 = Number(record.projects2025) || 0;
      const projects2024 = Number(record.projects2024) || 0;
      const recent = projects2026 > 0
        ? `${projects2026} in 2026`
        : projects2025 > 0
          ? `${projects2025} in 2025`
          : `${projects2024} in 2024`;
      return `${country}: ${recent}, ${total} total`;
    })
    .join("; ");
}

function formatPositionRegion(region) {
  if (!region || !region.name || !region.totalProjects) {
    return "No evidence";
  }
  return `${region.name}: ${region.totalProjects} projects since 2024`;
}

function getDetailStatusDescriptor(opportunity) {
  if (isExpiringSoon(opportunity)) {
    return { tone: "expiring", label: "Expiring soon" };
  }
  if (opportunity.actionStatus === "applied") {
    return { tone: "applied", label: "Applied" };
  }
  if (opportunity.actionStatus === "missed") {
    return { tone: "missed", label: "Missed" };
  }
  if (opportunity.actionStatus === "pending") {
    return { tone: "live", label: "Pending" };
  }
  if (opportunity.status === "expired") {
    return { tone: "missed", label: "Expired" };
  }
  if (opportunity.status === "stale") {
    return { tone: "missed", label: "Archived upstream" };
  }
  return { tone: "live", label: "Live" };
}

function getStatusLabel(opportunity) {
  if (isExpiringSoon(opportunity)) {
    return "Expiring";
  }

  if (opportunity.actionStatus) {
    return getActionLabel(opportunity.actionStatus, opportunity.missedReason);
  }

  if (opportunity.status === "expired") {
    return "Expired";
  }

  if (opportunity.status === "stale") {
    return "Archived upstream";
  }

  return "Live";
}

function getStatusBadgeLabel(opportunity) {
  if (opportunity.actionStatus === "reviewed") {
    return "Reviewed";
  }
  if (opportunity.actionStatus === "missed") {
    return "Missed";
  }
  return getStatusLabel(opportunity);
}

function getStatusTone(opportunity) {
  if (isExpiringSoon(opportunity)) {
    return "expiring";
  }
  if (opportunity.actionStatus === "reviewed") {
    return "reviewed";
  }
  if (opportunity.actionStatus === "applied") {
    return "applied";
  }
  if (opportunity.actionStatus === "pending") {
    return "pending";
  }
  if (opportunity.actionStatus === "missed") {
    return "missed";
  }
  if (opportunity.status === "expired") {
    return "expired";
  }
  return "live";
}

function getFitBadgeLabel(opportunity) {
  const fitScore = Number(opportunity.fitScore) || 0;
  if (fitScore >= 70) {
    return "High";
  }
  if (fitScore >= 40) {
    return "Medium";
  }
  return "Low";
}

function formatCompactDate(value) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
  }).format(date);
}

function getDeadlineMeta(opportunity) {
  if (!opportunity.deadline) {
    return { label: "No deadline", tone: "default" };
  }

  const days = getDaysUntilDeadline(opportunity.deadline);
  const base = formatCompactDate(opportunity.deadline);
  if (days === null) {
    return { label: base, tone: "default" };
  }
  if (days <= 1) {
    return { label: days < 0 ? `${base} · passed` : `${base} · ${Math.max(days, 0)}d`, tone: "critical" };
  }
  if (days <= 7) {
    return { label: `${base} · ${days}d`, tone: "warning" };
  }
  return { label: base, tone: "default" };
}

function getDetailTargetState(opportunity) {
  if (opportunity.actionStatus === "reviewed") {
    return "reviewed";
  }
  if (opportunity.actionStatus === "applied") {
    return "applied";
  }
  if (opportunity.actionStatus === "missed") {
    return "missed";
  }
  if (opportunity.status === "expired" || opportunity.status === "stale") {
    return "missed";
  }
  return "live";
}

function syncDetailTargetStateUi() {
  const targetState = detailTargetState.value;
  const opportunity = allOpportunities.find((item) => item.id === selectedOpportunityId);
  const currentStatus = opportunity ? getStatusLabel(opportunity) : "Live";

  detailStateButtons.forEach((button) => {
    const isActive = button.dataset.detailTargetState === targetState;
    button.classList.toggle("is-active", isActive);
    button.dataset.state = button.dataset.detailTargetState || "";
    button.disabled = detailActionBusy;
  });

  if (targetState !== "missed") {
    detailMissedReason.value = "";
  }

  detailMissedField.classList.toggle("is-active", targetState === "missed");
  detailMissedReason.disabled = detailActionBusy || targetState !== "missed";
  detailTargetHelp.textContent = `Currently ${currentStatus.toLowerCase()} — select a new state to update.`;
  updateDetailActionButtonState();
}

function getTargetStateSuccessMessage(targetState, missedReason) {
  if (targetState === "live") {
    return "Opportunity moved to Live.";
  }
  if (targetState === "reviewed") {
    return "Opportunity marked as Reviewed — ops team notified it has been looked at.";
  }
  if (targetState === "applied") {
    return "Opportunity marked as Applied.";
  }
  if (targetState === "pending") {
    return "Opportunity marked as Pending.";
  }
  if (targetState === "missed") {
    return `Opportunity marked as Missed${missedReason ? `: ${getMissedReasonLabel(missedReason)}` : ""}.`;
  }
  if (targetState === "expired") {
    return "Opportunity marked as Expired.";
  }
  if (targetState === "archived") {
    return "Opportunity archived as upstream-removed.";
  }
  return "Opportunity updated.";
}

function getActionLabel(actionStatus, missedReason = "") {
  if (actionStatus === "reviewed") {
    return "Reviewed";
  }
  if (actionStatus === "pending") {
    return "Pending";
  }
  if (actionStatus === "applied") {
    return "Applied";
  }
  if (actionStatus === "missed") {
    return `Missed${missedReason ? `: ${getMissedReasonLabel(missedReason)}` : ""}`;
  }
  return "Live";
}

function getMissedReasonLabel(reason) {
  if (reason === "expired") {
    return "Expired";
  }

  if (reason === "not_relevant") {
    return "Not relevant";
  }

  if (reason === "not_interested") {
    return "Not interested";
  }

  if (reason === "duplicate") {
    return "Duplicate";
  }

  return "";
}

function getResultTabConfig(tab) {
  if (tab === "applied") {
    return {
      label: "Applied",
      title: "Applied",
      summary: (count) =>
        count
          ? `${count} opportunities were marked as applied and moved out of the live queue.`
          : "Applied opportunities will show up here.",
    };
  }

  if (tab === "pending") {
    return {
      label: "Pending",
      title: "Pending",
      summary: (count) =>
        count
          ? `${count} opportunities are being reviewed or prepared but not submitted yet.`
          : "Pending opportunities will show up here while they are in progress.",
    };
  }

  if (tab === "missed") {
    return {
      label: "Missed",
      title: "Missed",
      summary: (count) =>
        count
          ? `${count} opportunities were missed, expired, or archived after disappearing from the source.`
          : "Missed or archived opportunities will show up here with a reason.",
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
  if (tab === "applied") {
    return "applied";
  }

  if (tab === "pending") {
    return "pending";
  }

  if (tab === "missed") {
    return "missed";
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
  setRecipientChips(Array.isArray(settings.recipientEmails) ? settings.recipientEmails : []);
  notificationEmailInput.value = "";
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
  const invalidRecipients = notificationRecipientChips.filter((chip) => !chip.valid);
  if (invalidRecipients.length > 0) {
    throw new Error("Fix invalid recipient emails before saving.");
  }

  return {
    enabled: notificationEnabled.checked,
    newTenderEnabled: notificationNewEnabled.checked,
    expiryAlertEnabled: notificationExpiryEnabled.checked,
    recipientEmails: notificationRecipientChips.map((chip) => chip.value),
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

function setNotificationSettingsBusy(isBusy, statusText = null) {
  notificationSettingsBusy = isBusy;
  [
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
  notificationEmailInput.disabled = isBusy;
  renderRecipientChips();

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
  const storedView = localStorage.getItem(STORAGE_KEYS.view);
  const storedTheme = localStorage.getItem(STORAGE_KEYS.theme) || "dark";
  setTheme(storedTheme, { persist: false });
  setViewMode(storedView === "cards" ? "cards" : "table", { persist: false });
  updateTenderSearchUi();
  updateTableSortUi();
  renderRecipientChips();
}

function updateSyncMeta(sync, sourceResultsOverride = null) {
  if (!sync || !sync.lastSyncedAt) {
    lastUpdated.textContent = "Last synced: never";
    return;
  }

  lastUpdated.textContent = `Last synced ${formatRelativeTime(sync.lastSyncedAt)}`;
  if (Array.isArray(sourceResultsOverride) && sourceResultsOverride.length > 0) {
    debugLog("updateSyncMeta:sourceResults", sourceResultsOverride);
  }
}

function getSourceCountsForItems(items) {
  return DISPLAY_SOURCES.map((source) => ({
    source,
    itemCount: items.filter((item) => {
      const sourceList = Array.isArray(item?.sourceList) && item.sourceList.length > 0
        ? item.sourceList
        : [item?.source];
      return sourceList.includes(source);
    }).length,
  }));
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

      return `
        <div class="source-health-pill source-health-pill--${tone}">
          <span class="source-health-dot"></span>
          <span class="source-health-pill__name">${sourceName}</span>
          <span class="source-health-pill__meta" style="color:var(--text-faint)">${itemLabel}</span>
        </div>
      `;
    })
    .join("");
}

function getSourceHealthTone(result) {
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

function setActiveTab(tab, options = {}) {
  activeBucketRequestId += 1;
  currentOpportunityTab = TAB_QUERY_MAP.has(tab) ? tab : "results";
  debugLog("setActiveTab", {
    tab: currentOpportunityTab,
    activeBucketRequestId,
    currentBucket: getApiBucketForTab(currentOpportunityTab),
  });
  closeDetailModal();

  if (options.updateUrl !== false) {
    syncTabUrl(currentOpportunityTab);
  }

  resultsTab.classList.toggle("active", currentOpportunityTab === "results");
  pendingTab.classList.toggle("active", currentOpportunityTab === "pending");
  appliedTab.classList.toggle("active", currentOpportunityTab === "applied");
  missedTab.classList.toggle("active", currentOpportunityTab === "missed");

  resultsTab.setAttribute("aria-selected", String(currentOpportunityTab === "results"));
  pendingTab.setAttribute("aria-selected", String(currentOpportunityTab === "pending"));
  appliedTab.setAttribute("aria-selected", String(currentOpportunityTab === "applied"));
  missedTab.setAttribute("aria-selected", String(currentOpportunityTab === "missed"));

  resultsPanel.hidden = false;
  currentTablePage = 1;

  if (!options.silent) {
    reloadOpportunitiesFromApi().catch(() => {
      debugLog("setActiveTab:reload-failed", { tab: currentOpportunityTab });
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

function sanitizeOpportunityTitle(title) {
  return String(title || "Opportunity")
    .replace(/\bopen in a new window\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function getDetailSequence() {
  return Array.isArray(latestSortedOpportunities) && latestSortedOpportunities.length > 0
    ? latestSortedOpportunities
    : allOpportunities;
}

function getSelectedOpportunity() {
  return allOpportunities.find((item) => item.id === selectedOpportunityId) || null;
}

function getSelectedOpportunityIndex() {
  const sequence = getDetailSequence();
  return sequence.findIndex((item) => item.id === selectedOpportunityId);
}

function syncDetailNavigation() {
  const sequence = getDetailSequence();
  const currentIndex = getSelectedOpportunityIndex();
  const total = sequence.length || allOpportunities.length || 1;
  const displayIndex = currentIndex >= 0 ? currentIndex + 1 : 1;
  detailKicker.textContent = `${displayIndex} of ${total} · Opportunity Detail`;
  detailPrevButton.disabled = currentIndex <= 0;
  detailNextButton.disabled = currentIndex === -1 || currentIndex >= total - 1;
}

function openAdjacentDetailModal(delta) {
  const sequence = getDetailSequence();
  const currentIndex = getSelectedOpportunityIndex();
  const nextOpportunity = sequence[currentIndex + delta];
  if (!nextOpportunity) {
    return;
  }
  openDetailModal(nextOpportunity.id);
}

function updateDetailActionButtonState() {
  const targetState = detailTargetState.value;
  const missedReason = detailMissedReason.value;
  const blocked =
    detailActionBusy ||
    (targetState === "missed" && !missedReason);

  detailActionSave.disabled = blocked;
  detailActionSave.setAttribute("aria-disabled", String(blocked));
}

function showToast(message, tone = "success") {
  if (!toast) {
    return;
  }
  window.clearTimeout(toastTimeoutId);
  toast.hidden = false;
  toast.className = `toast toast--${tone}`;
  toast.textContent = message;
  toastTimeoutId = window.setTimeout(() => {
    toast.hidden = true;
  }, 2600);
}

function openDetailModal(opportunityId) {
  const opportunity = allOpportunities.find((item) => item.id === opportunityId);
  if (!opportunity) {
    return;
  }

  selectedOpportunityId = opportunityId;
  detailActionBusy = false;
  const fitDescriptor = getDetailFitDescriptor(opportunity);
  const positionDescriptor = getPositionDescriptor(opportunity);
  const warmthDescriptor = getWarmthDescriptor(opportunity);
  const statusDescriptor = getDetailStatusDescriptor(opportunity);
  const deadlineMeta = getDetailDeadlineMeta(opportunity.deadline);
  const countryLabel = getOpportunityCountryLabel(opportunity);
  const detailTitleParts = [
    opportunity.organization || "N/A",
    getOpportunityCountryLabel(opportunity, { short: true }),
    getOpportunityRawId(opportunity),
  ];

  detailContextLine.textContent = detailTitleParts.join(" · ");
  detailTitle.textContent = getOpportunityDisplayTitle(opportunity);
  detailTitleLink.href = opportunity.link || "#";
  detailTitleLink.hidden = !opportunity.link;
  detailFit.className = `detail-badge detail-badge--fit detail-badge--${fitDescriptor.tone}`;
  detailFit.textContent = fitDescriptor.label;
  detailPositionBadge.className = `position-badge position-badge--${positionDescriptor.tone}`;
  detailPositionBadge.textContent = positionDescriptor.label;
  detailPositionSummary.textContent = positionDescriptor.summary;
  detailPositionExact.textContent = formatPositionRecords(positionDescriptor.evidence.exact);
  detailPositionNearby.textContent = formatPositionRecords(positionDescriptor.evidence.neighbors);
  detailPositionRegion.textContent = formatPositionRegion(positionDescriptor.evidence.region);
  detailWarmthBadge.className = `warmth-badge warmth-badge--${warmthDescriptor.tone}`;
  detailWarmthBadge.textContent = warmthDescriptor.label;
  detailWarmthSummary.textContent = warmthDescriptor.summary;
  detailWarmthClient.textContent = formatWarmthClient(warmthDescriptor.evidence.client);
  detailWarmthGroup.textContent = formatWarmthGroup(warmthDescriptor.evidence);
  detailStatus.className = `detail-badge detail-badge--status detail-badge--${statusDescriptor.tone}`;
  detailStatus.textContent = statusDescriptor.label;
  detailSource.textContent = getOpportunitySourceDetail(opportunity);
  detailOrganization.textContent = opportunity.organization || "N/A";
  detailCountries.textContent = countryLabel;
  detailDeadline.className = `detail-meta-value detail-meta-value--${deadlineMeta.tone}`;
  detailDeadline.innerHTML = `${escapeHtml(deadlineMeta.label)}${deadlineMeta.suffix ? ` <small>${escapeHtml(deadlineMeta.suffix)}</small>` : ""}`;
  detailType.textContent = getDetailTypeLabel(opportunity);
  detailAdded.textContent = formatDate(opportunity.addedAt);
  detailSourceMeta.textContent = getOpportunitySourceDetail(opportunity);
  detailNotes.value = opportunity.actionNotes || "";
  detailTargetState.value = getDetailTargetState(opportunity);
  detailMissedReason.value = opportunity.missedReason || "";
  detailLink.href = opportunity.link || "#";
  detailLink.hidden = !opportunity.link;
  detailNotes.disabled = false;
  syncDetailNavigation();
  syncDetailTargetStateUi();
  detailModal.hidden = false;
  syncGlobalModalState();
  detailCloseButton.focus();
}

function closeDetailModal() {
  detailModal.hidden = true;
  selectedOpportunityId = null;
  detailActionBusy = false;
  syncGlobalModalState();
}

function openSourcesDrawer() {
  sourcesDrawer.hidden = false;
  syncGlobalModalState();
}

function closeSourcesDrawer() {
  sourcesDrawer.hidden = true;
  syncGlobalModalState();
}

function openSettingsModal() {
  settingsModal.hidden = false;
  syncGlobalModalState();
  notificationEmailInput.focus();
}

function closeSettingsModal() {
  settingsModal.hidden = true;
  syncGlobalModalState();
}

function syncGlobalModalState() {
  document.body.classList.toggle("modal-open", !detailModal.hidden || !settingsModal.hidden || !sourcesDrawer.hidden);
}

async function handleDetailActionSubmit() {
  if (!selectedOpportunityId) {
    return;
  }

  const notes = detailNotes.value.trim();
  const targetState = detailTargetState.value;
  const missedReason = detailMissedReason.value;

  if (targetState === "missed" && !missedReason) {
    setMessage("Select why this opportunity was missed.", "error");
    detailMissedReason.focus();
    return;
  }

  toggleDetailActionButtons(true);

  try {
    const updatedItem = await saveOpportunityAction(selectedOpportunityId, targetState, notes, missedReason);
    if (updatedItem) {
      allOpportunities = allOpportunities.map((item) =>
        item.id === updatedItem.id ? updatedItem : item
      );
      await reloadOpportunitiesFromApi();
      const successMessage = getTargetStateSuccessMessage(targetState, missedReason);
      setMessage(
        successMessage,
        "success"
      );
      showToast(successMessage, "success");
      closeDetailModal();
    }
  } catch (error) {
    setMessage(error.message || "Could not save the opportunity action.", "error");
    showToast(error.message || "Could not save the opportunity action.", "error");
  } finally {
    toggleDetailActionButtons(false);
  }
}

function toggleDetailActionButtons(disabled) {
  detailActionBusy = disabled;
  detailTargetState.disabled = disabled;
  detailStateButtons.forEach((button) => {
    button.disabled = disabled;
  });
  detailNotes.disabled = disabled;
  detailMissedReason.disabled = disabled || detailTargetState.value !== "missed";
  if (disabled) {
    detailPrevButton.disabled = true;
    detailNextButton.disabled = true;
  } else {
    syncDetailNavigation();
  }
  updateDetailActionButtonState();
}

function setTheme(theme, options = {}) {
  const nextTheme = theme === "light" ? "light" : "dark";
  document.documentElement.dataset.theme = nextTheme;
  document.documentElement.style.colorScheme = nextTheme;
  document.body.dataset.theme = nextTheme;

  if (themeLightButton && themeDarkButton) {
    themeLightButton.classList.toggle("active", nextTheme === "light");
    themeDarkButton.classList.toggle("active", nextTheme === "dark");
  }

  if (themeToggleButton) {
    themeToggleButton.setAttribute("aria-label",
      nextTheme === "dark" ? "Switch to light mode" : "Switch to dark mode"
    );
  }

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

function handleTenderSearchInput() {
  currentTablePage = 1;
  updateTenderSearchUi();
  applyTableState();
}

function clearTenderSearch() {
  if (!tenderSearchInput.value) {
    return;
  }
  tenderSearchInput.value = "";
  currentTablePage = 1;
  updateTenderSearchUi();
  applyTableState();
  tenderSearchInput.focus();
}

function updateTenderSearchUi() {
  tenderSearchClearButton.hidden = !tenderSearchInput.value.trim();
}

function matchesTenderSearch(opportunity, rawQuery) {
  const query = String(rawQuery || "").trim().toLowerCase();
  if (!query) {
    return true;
  }

  return buildTenderSearchText(opportunity).includes(query);
}

function buildTenderSearchText(opportunity) {
  const sourceList = Array.isArray(opportunity.sourceList) ? opportunity.sourceList.join(" ") : "";
  const countryList = Array.isArray(opportunity.countryList) ? opportunity.countryList.join(" ") : "";
  const position = opportunity.fairpicturePosition || {};
  const warmth = opportunity.clientWarmth || {};
  const noticeIds = extractNoticeIds(opportunity.link).join(" ");

  return [
    opportunity.title,
    opportunity.organization,
    opportunity.type,
    opportunity.link,
    opportunity.source,
    sourceList,
    countryList,
    position.label,
    position.summary,
    warmth.label,
    warmth.summary,
    noticeIds,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function extractNoticeIds(link) {
  const matches = String(link || "").match(/\/Notice\/(\d+)/gi) || [];
  return matches
    .map((value) => {
      const match = value.match(/(\d+)/);
      return match ? match[1] : "";
    })
    .filter(Boolean);
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
