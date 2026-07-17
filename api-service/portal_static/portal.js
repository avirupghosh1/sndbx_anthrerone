const THEME_STORAGE_KEY = "sndbx.theme";

function getStoredTheme() {
  try {
    const value = window.localStorage.getItem(THEME_STORAGE_KEY);
    return value === "dark" || value === "light" ? value : "";
  } catch (_) {
    return "";
  }
}

function storeTheme(theme) {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch (_) {
    return;
  }
}

function applyPortalTheme(theme) {
  const normalized = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", normalized);
  document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
    const isDark = normalized === "dark";
    button.setAttribute("aria-pressed", isDark ? "true" : "false");
    button.setAttribute("aria-label", isDark ? "Switch to light mode" : "Switch to dark mode");
    const text = button.querySelector(".theme-toggle-text");
    if (text) text.textContent = isDark ? "Light" : "Dark";
  });
}

applyPortalTheme(getStoredTheme() || "light");

let portalPopoverCounter = 0;

function getPopoverOverlay(details) {
  if (!details) return null;
  let overlay = details._portalOverlay || details.querySelector(".popover-overlay");
  if (!overlay) return null;
  if (!details._portalOverlay) {
    details._portalOverlay = overlay;
    overlay._portalOwner = details;
    portalPopoverCounter += 1;
    details.setAttribute("data-popover-portal", "popover-" + portalPopoverCounter);
  }
  return overlay;
}

function syncPopoverOverlay(details) {
  const overlay = getPopoverOverlay(details);
  if (!overlay) return;
  overlay.classList.toggle("portal-popover-open", Boolean(details.open));
}

function closeValuePopover(details) {
  if (!details) return;
  details.removeAttribute("open");
  syncPopoverOverlay(details);
}

function closePopoverFromTarget(target) {
  const overlay = target.closest(".popover-overlay");
  const details = overlay && overlay._portalOwner ? overlay._portalOwner : target.closest(".value-popover");
  closeValuePopover(details);
}

function mountPortalLayers() {
  document.querySelectorAll(".portal-modal").forEach(function (modal) {
    if (modal.parentElement !== document.body) document.body.appendChild(modal);
  });
  document.querySelectorAll(".value-popover").forEach(function (details) {
    const overlay = getPopoverOverlay(details);
    if (!overlay) return;
    if (overlay.parentElement !== document.body) document.body.appendChild(overlay);
    syncPopoverOverlay(details);
  });
}

document.addEventListener("DOMContentLoaded", function () {
  applyPortalTheme(getStoredTheme() || document.documentElement.getAttribute("data-theme") || "light");
  mountPortalLayers();
});

if (document.readyState !== "loading") mountPortalLayers();

document.addEventListener("click", function (event) {
  const toggle = event.target.closest("[data-theme-toggle]");
  if (!toggle) return;
  const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
  applyPortalTheme(next);
  storeTheme(next);
});

window.addEventListener("storage", function (event) {
  if (event.key === THEME_STORAGE_KEY) applyPortalTheme(event.newValue || "light");
});

function openPortalModal(name) {
  mountPortalLayers();
  const modal = document.querySelector('.portal-modal[data-modal="' + name + '"]');
  if (!modal) return null;
  modal.setAttribute("aria-hidden", "false");
  return modal;
}

function closePortalModal(modal) {
  if (!modal) return;
  modal.setAttribute("aria-hidden", "true");
  if (modal._portalStopPolling) modal._portalStopPolling();
}

document.addEventListener("click", function (event) {
  const closeButton = event.target.closest(".popover-close");
  if (closeButton) {
    closePopoverFromTarget(closeButton);
    return;
  }
  const backdrop = event.target.closest(".popover-backdrop");
  if (backdrop) {
    closePopoverFromTarget(backdrop);
  }
});

document.addEventListener(
  "toggle",
  function (event) {
    const current = event.target;
    if (!current.matches(".value-popover")) return;
    if (!current.open) {
      syncPopoverOverlay(current);
      return;
    }
    const overlay = getPopoverOverlay(current);
    if (overlay && overlay.parentElement !== document.body) document.body.appendChild(overlay);
    syncPopoverOverlay(current);
    document.querySelectorAll(".value-popover[open]").forEach(function (details) {
      if (details !== current) closeValuePopover(details);
    });
  },
  true
);

document.addEventListener("click", function (event) {
  const close = event.target.closest("[data-modal-close]");
  if (close) {
    closePortalModal(close.closest(".portal-modal"));
    return;
  }
  const gateway = event.target.closest("[data-gateway-detail]");
  if (gateway) openPortalModal(gateway.getAttribute("data-gateway-detail"));
});

document.querySelectorAll(".table-panel").forEach(function (panel) {
  const input = panel.querySelector(".table-search");
  const table = panel.querySelector("[data-filter-table]");
  if (!input || !table) return;
  const rows = Array.from(table.querySelectorAll("tbody tr"));
  input.addEventListener("input", function () {
    const needle = input.value.trim().toLowerCase();
    rows.forEach(function (row) {
      const haystack = (row.textContent || "").toLowerCase();
      row.hidden = needle !== "" && !haystack.includes(needle);
    });
  });
});

document.querySelectorAll("[data-event-table]").forEach(function (table) {
  const section = table.closest(".observability-section") || document;
  const search = section.querySelector(".event-search");
  const filters = Array.from(section.querySelectorAll(".event-filter"));
  const rows = Array.from(table.querySelectorAll("tbody tr"));

  function applyEventFilters() {
    const needle = search ? search.value.trim().toLowerCase() : "";
    const selected = {};
    filters.forEach(function (filter) {
      const key = filter.getAttribute("data-event-filter");
      if (key) selected[key] = filter.value.trim().toLowerCase();
    });
    rows.forEach(function (row) {
      const text = (row.textContent || "").toLowerCase();
      const severity = (row.getAttribute("data-severity") || "").toLowerCase();
      const category = (row.getAttribute("data-category") || "").toLowerCase();
      const hideByText = needle && !text.includes(needle);
      const hideBySeverity = selected.severity && severity !== selected.severity;
      const hideByCategory = selected.category && category !== selected.category;
      row.hidden = Boolean(hideByText || hideBySeverity || hideByCategory);
    });
  }

  if (search) search.addEventListener("input", applyEventFilters);
  filters.forEach(function (filter) {
    filter.addEventListener("change", applyEventFilters);
  });
});

function setText(root, selector, value) {
  const node = root.querySelector(selector);
  if (node) node.textContent = value || "";
}

function renderProgress(modal, data) {
  setText(modal, "[data-build-title]", data.template_id || data.build_id || "Build");
  setText(modal, "[data-build-subtitle]", data.build_id || "");
  setText(modal, "[data-build-status]", data.status || "unknown");
  setText(modal, "[data-build-phase]", data.progress ? data.progress.phase : "");
  setText(modal, "[data-build-comment]", data.progress ? data.progress.latest_comment : "");
  const percent = data.progress ? Number(data.progress.percent || 0) : 0;
  const fill = modal.querySelector("[data-build-progress]");
  if (fill) fill.value = Math.max(0, Math.min(100, percent));
  setText(modal, "[data-build-percent]", Math.round(percent) + "%");
  const status = modal.querySelector("[data-build-status]");
  if (status) {
    status.className = "status-pill";
    if (data.status === "success") status.classList.add("success");
    if (data.status === "failed") status.classList.add("danger");
  }
}

function renderLogs(modal, data) {
  setText(modal, "[data-build-title]", data.template_id || data.build_id || "Build");
  setText(modal, "[data-build-subtitle]", data.build_id || "");
  const viewer = modal.querySelector("[data-log-viewer]");
  if (!viewer) return;
  viewer.textContent = "";
  const lines = data.log_lines || [];
  if (!lines.length) {
    const empty = document.createElement("div");
    empty.className = "log-empty";
    empty.textContent = "No build log captured.";
    viewer.appendChild(empty);
    return;
  }
  lines.forEach(function (line) {
    const row = document.createElement("div");
    row.className = "log-line " + (line.severity || "info");
    const number = document.createElement("span");
    number.className = "log-number";
    number.textContent = String(line.number || "");
    const text = document.createElement("span");
    text.className = "log-text";
    text.textContent = line.text || "";
    row.appendChild(number);
    row.appendChild(text);
    viewer.appendChild(row);
  });
}

function pollBuild(modal, buildId, renderer) {
  let stopped = false;
  let timer = null;
  modal._portalStopPolling = function () {
    stopped = true;
    if (timer) window.clearTimeout(timer);
  };

  function tick() {
    fetch("/portal/templates/builds/" + encodeURIComponent(buildId) + ".json", {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    })
      .then(function (response) {
        if (!response.ok) throw new Error("build request failed");
        return response.json();
      })
      .then(function (data) {
        if (stopped || modal.getAttribute("aria-hidden") === "true") return;
        modal._latestBuildData = data;
        renderer(modal, data);
        if (data.status !== "success" && data.status !== "failed") {
          timer = window.setTimeout(tick, 1500);
        }
      })
      .catch(function () {
        if (!stopped) timer = window.setTimeout(tick, 2500);
      });
  }
  tick();
}

document.addEventListener("click", function (event) {
  const action = event.target.closest(".build-action");
  if (!action) return;
  const buildId = action.getAttribute("data-build-id");
  const kind = action.getAttribute("data-build-modal");
  const modal = openPortalModal(kind === "logs" ? "build-logs" : "build-progress");
  if (!buildId || !modal) return;
  if (modal._portalStopPolling) modal._portalStopPolling();
  if (kind === "logs") pollBuild(modal, buildId, renderLogs);
  else pollBuild(modal, buildId, renderProgress);
});

document.addEventListener("click", function (event) {
  const copy = event.target.closest("[data-copy-text], .copy-logs-button");
  if (!copy) return;
  let text = copy.getAttribute("data-copy-text") || "";
  if (!text && copy.classList.contains("copy-logs-button")) {
    const modal = copy.closest(".portal-modal");
    const data = modal ? modal._latestBuildData : null;
    text = data && data.log_lines ? data.log_lines.map(function (line) { return line.text || ""; }).join("\n") : "";
  }
  if (!text || !navigator.clipboard) return;
  navigator.clipboard.writeText(text);
});

document.addEventListener("click", function (event) {
  const tab = event.target.closest("[data-response-target]");
  if (!tab) return;
  const box = tab.closest(".api-response-box");
  if (!box) return;
  const target = tab.getAttribute("data-response-target");
  box.querySelectorAll(".api-response-tab").forEach(function (item) {
    item.classList.toggle("active", item === tab);
  });
  box.querySelectorAll("[data-response-panel]").forEach(function (panel) {
    panel.classList.toggle("active", panel.getAttribute("data-response-panel") === target);
  });
});

document.addEventListener("click", function (event) {
  const button = event.target.closest("[data-event-metadata]");
  if (!button) return;
  const modal = openPortalModal("event-metadata");
  const target = modal ? modal.querySelector(".metadata-view") : null;
  if (target) target.textContent = button.getAttribute("data-event-metadata") || "{}";
});

document.addEventListener("keydown", function (event) {
  if (event.key === "Escape") {
    document.querySelectorAll('.portal-modal[aria-hidden="false"]').forEach(closePortalModal);
    document.querySelectorAll(".value-popover[open]").forEach(function (details) {
      closeValuePopover(details);
    });
    return;
  }
  if (event.key !== "/" || event.target.matches("input, textarea, select")) return;
  const input = document.querySelector(".table-search, .event-search");
  if (!input) return;
  event.preventDefault();
  input.focus();
});
