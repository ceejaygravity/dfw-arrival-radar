const state = {
  data: null,
  query: "",
  earlyOnly: false,
  selectedTerminals: [],
};

const summaryGrid = document.getElementById("summaryGrid");
const terminalStack = document.getElementById("terminalStack");
const statusBanner = document.getElementById("statusBanner");
const searchInput = document.getElementById("searchInput");
const terminalFilterOptions = document.getElementById("terminalFilterOptions");
const earlyOnlyToggle = document.getElementById("earlyOnlyToggle");
const refreshButton = document.getElementById("refreshButton");
const dateLabel = document.getElementById("dateLabel");
const fetchedAtLabel = document.getElementById("fetchedAtLabel");
const sourceLabel = document.getElementById("sourceLabel");
const flightRowTemplate = document.getElementById("flightRowTemplate");


function formatDate(value) {
  if (!value) return "Unknown";
  return new Date(`${value}T12:00:00`).toLocaleDateString(undefined, {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  });
}


function formatTimestamp(value) {
  if (!value) return "Unknown";
  return new Date(value).toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}


function statusClass(status) {
  const value = (status || "").toLowerCase();
  if (value.includes("delayed")) return "is-delayed";
  if (value.includes("landed")) return "is-landed";
  if (value.includes("on-time")) return "is-on-time";
  return "";
}


function matchesQuery(flight, query) {
  if (!query) return true;
  const haystack = [
    flight.flightNumber,
    ...(flight.codeshares || []),
    flight.airline,
    flight.origin,
    flight.originCode,
    flight.gate,
    flight.terminal,
    flight.status,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(query);
}


function allTerminalNames() {
  return (state.data?.terminals || []).map((terminal) => terminal.terminal);
}


function terminalIsSelected(terminalName) {
  if (!state.selectedTerminals.length) return true;
  return state.selectedTerminals.includes(terminalName);
}


function filterTerminals(terminals) {
  const query = state.query.trim().toLowerCase();

  return terminals
    .filter((terminal) => terminalIsSelected(terminal.terminal))
    .map((terminal) => {
      const gates = terminal.gates
        .map((gate) => {
          const flights = gate.flights.filter((flight) => {
            if (state.earlyOnly && !flight.isEarly) return false;
            return matchesQuery(flight, query);
          });

          return {
            ...gate,
            flights,
            totalFlights: flights.length,
            earlyFlights: flights.filter((flight) => flight.isEarly).length,
          };
        })
        .filter((gate) => gate.flights.length > 0)
        .sort((a, b) => {
          if (b.earlyFlights !== a.earlyFlights) {
            return b.earlyFlights - a.earlyFlights;
          }
          return a.gate.localeCompare(b.gate, undefined, { numeric: true });
        });

      return {
        ...terminal,
        gates,
        totalFlights: gates.reduce((sum, gate) => sum + gate.totalFlights, 0),
        earlyFlights: gates.reduce((sum, gate) => sum + gate.earlyFlights, 0),
        gateCount: gates.length,
      };
    })
    .filter((terminal) => terminal.gates.length > 0);
}


function summarizeFiltered(terminals) {
  const totalFlights = terminals.reduce((sum, terminal) => sum + terminal.totalFlights, 0);
  const earlyFlights = terminals.reduce((sum, terminal) => sum + terminal.earlyFlights, 0);
  const trackedGates = terminals.reduce((sum, terminal) => sum + terminal.gateCount, 0);
  const busiestTerminal = terminals.reduce((current, terminal) => {
    if (!current || terminal.totalFlights > current.totalFlights) {
      return terminal;
    }
    return current;
  }, null);

  return {
    totalFlights,
    earlyFlights,
    trackedGates,
    busiestTerminal: busiestTerminal?.terminal || null,
    busiestTerminalFlights: busiestTerminal?.totalFlights || 0,
  };
}


function renderSummary(summary) {
  const cards = [
    ["Flights today", String(summary.totalFlights ?? 0)],
    ["Early arrivals", String(summary.earlyFlights ?? 0)],
    ["Tracked gates", String(summary.trackedGates ?? 0)],
    [
      "Most arrival flights",
      summary.busiestTerminal
        ? `${summary.busiestTerminal} (${summary.busiestTerminalFlights})`
        : "N/A",
    ],
  ];

  summaryGrid.innerHTML = "";
  for (const [label, value] of cards) {
    const card = document.createElement("article");
    card.className = "summary-card";
    card.innerHTML = `
      <span class="summary-label">${label}</span>
      <strong class="summary-value">${value}</strong>
    `;
    summaryGrid.appendChild(card);
  }
}


function renderTerminalFilter() {
  if (!terminalFilterOptions) return;

  const terminalNames = allTerminalNames();
  terminalFilterOptions.innerHTML = "";

  const allButton = document.createElement("button");
  allButton.type = "button";
  allButton.className = "terminal-chip-button";
  if (!state.selectedTerminals.length) {
    allButton.classList.add("is-active");
  }
  allButton.textContent = "All terminals";
  allButton.addEventListener("click", () => {
    state.selectedTerminals = [];
    render();
  });
  terminalFilterOptions.appendChild(allButton);

  terminalNames.forEach((terminalName) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "terminal-chip-button";
    if (terminalIsSelected(terminalName)) {
      button.classList.add("is-active");
    }
    button.textContent = terminalName === "Unassigned" ? "Unassigned" : `Terminal ${terminalName}`;
    button.addEventListener("click", () => {
      const next = new Set(state.selectedTerminals);
      if (!state.selectedTerminals.length) {
        terminalNames.forEach((name) => next.add(name));
      }

      if (next.has(terminalName)) {
        next.delete(terminalName);
      } else {
        next.add(terminalName);
      }

      if (!next.size || next.size === terminalNames.length) {
        state.selectedTerminals = [];
      } else {
        state.selectedTerminals = terminalNames.filter((name) => next.has(name));
      }

      render();
    });
    terminalFilterOptions.appendChild(button);
  });
}


function buildFlightRow(flight) {
  const fragment = flightRowTemplate.content.cloneNode(true);
  const root = fragment.querySelector(".flight-row-card");
  const flightId = fragment.querySelector(".flight-id");
  const flightRoute = fragment.querySelector(".flight-route");
  const badgeStatus = fragment.querySelector(".badge-status");
  const badgeEarly = fragment.querySelector(".badge-early");
  const airline = fragment.querySelector(".meta-airline");
  const schedule = fragment.querySelector(".meta-schedule");
  const estimate = fragment.querySelector(".meta-estimate");

  const codeshareText = flight.codeshares?.length ? ` | ${flight.codeshares.join(", ")}` : "";
  flightId.textContent = `${flight.flightNumber}${codeshareText}`;
  flightRoute.textContent = `${flight.origin} (${flight.originCode}) to Gate ${flight.gateLabel}`;
  badgeStatus.textContent = flight.status || "Status pending";
  const statusToken = statusClass(flight.status);
  if (statusToken) {
    badgeStatus.classList.add(statusToken);
  }

  if (flight.isEarly) {
    badgeEarly.hidden = false;
    badgeEarly.textContent = flight.minutesEarly
      ? `${flight.minutesEarly} min early`
      : "Ahead of time";
  }

  airline.textContent = flight.airline || "Airline pending";
  schedule.textContent = `Scheduled ${flight.scheduledArrival || "TBD"}`;
  estimate.textContent = `Estimate ${flight.estimatedArrival || "TBD"}`;

  if (flight.isEarly) {
    root.classList.add("is-early");
  }

  return fragment;
}


function renderTerminals(terminals) {
  terminalStack.innerHTML = "";

  if (!terminals.length) {
    terminalStack.innerHTML = `
      <div class="empty-state">
        No flights match the current filter. Clear the search or turn off the early-only toggle.
      </div>
    `;
    return;
  }

  terminals.forEach((terminal, index) => {
    const section = document.createElement("section");
    section.className = "terminal-section reveal";
    section.style.animationDelay = `${120 + index * 60}ms`;

    const gateCards = terminal.gates
      .map((gate) => {
        const gateCard = document.createElement("article");
        gateCard.className = "gate-card";
        if (gate.earlyFlights > 0) {
          gateCard.classList.add("is-early");
        }

        const header = document.createElement("div");
        header.className = "gate-card-header";
        header.innerHTML = `
          <div>
            <h3 class="gate-id">${gate.gate}</h3>
            <div class="gate-meta">
              <span class="gate-count">${gate.totalFlights} flight${gate.totalFlights === 1 ? "" : "s"}</span>
              <span class="gate-count ${gate.earlyFlights ? "is-early" : ""}">
                ${gate.earlyFlights} early
              </span>
            </div>
          </div>
        `;

        const list = document.createElement("div");
        list.className = "flight-list";
        gate.flights.forEach((flight) => list.appendChild(buildFlightRow(flight)));

        gateCard.append(header, list);
        return gateCard;
      });

    const gateGrid = document.createElement("div");
    gateGrid.className = "gate-grid";
    gateCards.forEach((card) => gateGrid.appendChild(card));

    section.innerHTML = `
      <div class="terminal-header">
        <div>
          <h2 class="terminal-title">Terminal ${terminal.terminal}</h2>
          <p class="terminal-subtitle">Gate-level arrivals for the current DFW day board.</p>
        </div>
        <div class="terminal-stats">
          <span class="terminal-chip">${terminal.totalFlights} arrivals</span>
          <span class="terminal-chip">${terminal.earlyFlights} early</span>
          <span class="terminal-chip">${terminal.gateCount} gates</span>
        </div>
      </div>
    `;

    section.appendChild(gateGrid);
    terminalStack.appendChild(section);
  });
}


function renderErrors(errors) {
  if (errors?.length) {
    statusBanner.hidden = false;
    statusBanner.textContent =
      `Some flight details could not be refreshed (${errors.length} issue${errors.length === 1 ? "" : "s"}). ` +
      "The board is still showing the data that was available.";
    return;
  }
  statusBanner.hidden = true;
  statusBanner.textContent = "";
}


function render() {
  if (!state.data) return;
  const filteredTerminals = filterTerminals(state.data.terminals || []);
  renderTerminalFilter();
  renderSummary(summarizeFiltered(filteredTerminals));
  renderTerminals(filteredTerminals);
  renderErrors(state.data.errors || []);
  dateLabel.textContent = formatDate(state.data.date);
  fetchedAtLabel.textContent = formatTimestamp(state.data.fetchedAt);
  sourceLabel.textContent = state.data.source?.name || "Unknown";
}


async function loadData(forceRefresh = false) {
  refreshButton.disabled = true;
  refreshButton.textContent = forceRefresh ? "Refreshing..." : "Loading...";

  try {
    const response = await fetch(`/api/arrivals${forceRefresh ? "?refresh=1" : ""}`, {
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`Request failed with ${response.status}`);
    }

    state.data = await response.json();
    const availableTerminals = new Set(allTerminalNames());
    state.selectedTerminals = state.selectedTerminals.filter((terminal) =>
      availableTerminals.has(terminal),
    );
    render();
  } catch (error) {
    statusBanner.hidden = false;
    statusBanner.textContent = `Unable to load live arrivals: ${error.message}`;
  } finally {
    refreshButton.disabled = false;
    refreshButton.textContent = "Refresh live data";
  }
}


searchInput.addEventListener("input", (event) => {
  state.query = event.target.value || "";
  render();
});


earlyOnlyToggle.addEventListener("change", (event) => {
  state.earlyOnly = Boolean(event.target.checked);
  render();
});


refreshButton.addEventListener("click", () => {
  loadData(true);
});


loadData();
window.setInterval(() => loadData(false), 5 * 60 * 1000);
