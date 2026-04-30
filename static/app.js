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


function formatMinutes(minutes) {
  if (minutes == null) return "N/A";
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  if (!hours) return `${remainder}m`;
  if (!remainder) return `${hours}h`;
  return `${hours}h ${remainder}m`;
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
    flight.city,
    flight.cityCode,
    flight.gate,
    flight.terminal,
    flight.status,
    flight.direction,
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


function computeTurnWindows(arrivals, departures) {
  const windows = [];
  let departureIndex = 0;

  for (const arrival of arrivals) {
    const arrivalIso = arrival.turnReferenceTimeIso;
    if (!arrivalIso) continue;

    while (departureIndex < departures.length) {
      const departure = departures[departureIndex];
      const departureIso = departure.turnReferenceTimeIso;
      if (!departureIso) {
        departureIndex += 1;
        continue;
      }
      if (departureIso <= arrivalIso) {
        departureIndex += 1;
        continue;
      }

      const minutesBetween = Math.round(
        (new Date(departureIso).getTime() - new Date(arrivalIso).getTime()) / 60000,
      );
      let category = "standard";
      if (minutesBetween <= 60) {
        category = "quick";
      } else if (minutesBetween >= 180) {
        category = "long";
      }

      windows.push({
        arrivalFlightNumber: arrival.flightNumber,
        departureFlightNumber: departure.flightNumber,
        arrivalTime: arrival.turnReferenceTime,
        departureTime: departure.turnReferenceTime,
        minutesBetween,
        category,
      });
      departureIndex += 1;
      break;
    }
  }

  return windows;
}


function sortGateFlights(flights) {
  return [...flights].sort((a, b) => {
    const left = a.scheduledTimeIso || "9999";
    const right = b.scheduledTimeIso || "9999";
    return left.localeCompare(right, undefined, { numeric: true });
  });
}


function filterTerminals(terminals) {
  const query = state.query.trim().toLowerCase();

  return terminals
    .filter((terminal) => terminalIsSelected(terminal.terminal))
    .map((terminal) => {
      const gates = terminal.gates
        .map((gate) => {
          const arrivals = sortGateFlights(
            gate.arrivals.filter((flight) => {
              if (state.earlyOnly && !flight.isEarly) return false;
              return matchesQuery(flight, query);
            }),
          );
          const departures = sortGateFlights(
            gate.departures.filter((flight) => matchesQuery(flight, query)),
          );
          const turnWindows = computeTurnWindows(arrivals, departures);
          const turnMinutes = turnWindows.map((window) => window.minutesBetween);

          return {
            ...gate,
            arrivals,
            departures,
            turnWindows,
            arrivalCount: arrivals.length,
            departureCount: departures.length,
            totalFlights: arrivals.length + departures.length,
            earlyFlights: arrivals.filter((flight) => flight.isEarly).length,
            quickestTurnMinutes: turnMinutes.length ? Math.min(...turnMinutes) : null,
            longestTurnMinutes: turnMinutes.length ? Math.max(...turnMinutes) : null,
          };
        })
        .filter((gate) => gate.totalFlights > 0)
        .sort((a, b) => {
          const left = a.quickestTurnMinutes ?? Number.POSITIVE_INFINITY;
          const right = b.quickestTurnMinutes ?? Number.POSITIVE_INFINITY;
          if (left !== right) {
            return left - right;
          }
          return a.gate.localeCompare(b.gate, undefined, { numeric: true });
        });

      return {
        ...terminal,
        gates,
        totalFlights: gates.reduce((sum, gate) => sum + gate.totalFlights, 0),
        arrivalCount: gates.reduce((sum, gate) => sum + gate.arrivalCount, 0),
        departureCount: gates.reduce((sum, gate) => sum + gate.departureCount, 0),
        earlyFlights: gates.reduce((sum, gate) => sum + gate.earlyFlights, 0),
        gateCount: gates.length,
      };
    })
    .filter((terminal) => terminal.gates.length > 0);
}


function summarizeFiltered(terminals) {
  const totalArrivals = terminals.reduce((sum, terminal) => sum + terminal.arrivalCount, 0);
  const totalDepartures = terminals.reduce((sum, terminal) => sum + terminal.departureCount, 0);
  const earlyFlights = terminals.reduce((sum, terminal) => sum + terminal.earlyFlights, 0);
  const trackedGates = terminals.reduce((sum, terminal) => sum + terminal.gateCount, 0);
  const turnWindows = terminals.flatMap((terminal) =>
    terminal.gates.flatMap((gate) => gate.turnWindows),
  );

  return {
    totalArrivals,
    totalDepartures,
    earlyFlights,
    trackedGates,
    quickTurnWindows: turnWindows.filter((window) => window.category === "quick").length,
    longTurnWindows: turnWindows.filter((window) => window.category === "long").length,
  };
}


function renderSummary(summary) {
  const cards = [
    ["Arrivals today", String(summary.totalArrivals ?? 0)],
    ["Early arrivals", String(summary.earlyFlights ?? 0)],
    ["Departures today", String(summary.totalDepartures ?? 0)],
    ["Quick turns", String(summary.quickTurnWindows ?? 0)],
    ["Long gate gaps", String(summary.longTurnWindows ?? 0)],
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


function buildFlightRoute(flight) {
  if (flight.direction === "departure") {
    return `To ${flight.city} (${flight.cityCode}) from Gate ${flight.gateLabel}`;
  }
  return `${flight.city} (${flight.cityCode}) to Gate ${flight.gateLabel}`;
}


function buildFlightRow(flight) {
  const fragment = flightRowTemplate.content.cloneNode(true);
  const root = fragment.querySelector(".flight-row-card");
  const flightId = fragment.querySelector(".flight-id");
  const flightRoute = fragment.querySelector(".flight-route");
  const badgeDirection = fragment.querySelector(".badge-direction");
  const badgeStatus = fragment.querySelector(".badge-status");
  const badgeEarly = fragment.querySelector(".badge-early");
  const airline = fragment.querySelector(".meta-airline");
  const schedule = fragment.querySelector(".meta-schedule");
  const estimate = fragment.querySelector(".meta-estimate");

  const codeshareText = flight.codeshares?.length ? ` | ${flight.codeshares.join(", ")}` : "";
  flightId.textContent = `${flight.flightNumber}${codeshareText}`;
  flightRoute.textContent = buildFlightRoute(flight);
  badgeDirection.textContent = flight.directionLabel || flight.direction;
  badgeDirection.classList.add(flight.direction === "departure" ? "is-departure" : "is-arrival");
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
  schedule.textContent = `${flight.direction === "departure" ? "Departs" : "Scheduled"} ${flight.scheduledTime || "TBD"}`;
  estimate.textContent = flight.eventTime
    ? `${flight.direction === "departure" ? "Latest" : "Estimate"} ${flight.eventTime}`
    : `${flight.direction === "departure" ? "Latest" : "Estimate"} TBD`;

  root.classList.add(flight.direction === "departure" ? "is-departure" : "is-arrival");
  if (flight.isEarly) {
    root.classList.add("is-early");
  }

  return fragment;
}


function buildFlightSection(title, flights, emptyMessage) {
  const section = document.createElement("section");
  section.className = "gate-flow-section";

  const heading = document.createElement("div");
  heading.className = "gate-flow-header";
  heading.innerHTML = `<h4>${title}</h4><span>${flights.length}</span>`;
  section.appendChild(heading);

  if (!flights.length) {
    const empty = document.createElement("div");
    empty.className = "gate-flow-empty";
    empty.textContent = emptyMessage;
    section.appendChild(empty);
    return section;
  }

  const list = document.createElement("div");
  list.className = "flight-list";
  flights.forEach((flight) => list.appendChild(buildFlightRow(flight)));
  section.appendChild(list);
  return section;
}


function renderTurnSummary(gate) {
  const summary = document.createElement("div");
  summary.className = "turn-summary";

  if (!gate.turnWindows.length) {
    summary.innerHTML = `
      <div class="turn-pill">
        <span class="turn-label">Turn windows</span>
        <strong>Need both an arrival and later departure</strong>
      </div>
    `;
    return summary;
  }

  summary.innerHTML = `
    <div class="turn-pill">
      <span class="turn-label">Shortest turn</span>
      <strong>${formatMinutes(gate.quickestTurnMinutes)}</strong>
    </div>
    <div class="turn-pill">
      <span class="turn-label">Longest gap</span>
      <strong>${formatMinutes(gate.longestTurnMinutes)}</strong>
    </div>
  `;

  return summary;
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
        if (gate.quickestTurnMinutes != null && gate.quickestTurnMinutes <= 60) {
          gateCard.classList.add("has-quick-turn");
        }

        const header = document.createElement("div");
        header.className = "gate-card-header";
        header.innerHTML = `
          <div>
            <h3 class="gate-id">${gate.gate}</h3>
            <div class="gate-meta">
              <span class="gate-count">${gate.arrivalCount} arrival${gate.arrivalCount === 1 ? "" : "s"}</span>
              <span class="gate-count">${gate.departureCount} departure${gate.departureCount === 1 ? "" : "s"}</span>
              <span class="gate-count ${gate.earlyFlights ? "is-early" : ""}">
                ${gate.earlyFlights} early
              </span>
            </div>
          </div>
        `;

        const flowGrid = document.createElement("div");
        flowGrid.className = "gate-flow-grid";
        flowGrid.append(
          buildFlightSection("Arrivals", gate.arrivals, "No arrivals match the current filter."),
          buildFlightSection("Departures", gate.departures, "No departures match the current filter."),
        );

        gateCard.append(header, renderTurnSummary(gate), flowGrid);
        return gateCard;
      });

    const gateGrid = document.createElement("div");
    gateGrid.className = "gate-grid";
    gateCards.forEach((card) => gateGrid.appendChild(card));

    section.innerHTML = `
      <div class="terminal-header">
        <div>
          <h2 class="terminal-title">Terminal ${terminal.terminal}</h2>
          <p class="terminal-subtitle">Gate-level arrivals, departures, and likely turn windows for today's DFW board.</p>
        </div>
        <div class="terminal-stats">
          <span class="terminal-chip">${terminal.arrivalCount} arrivals</span>
          <span class="terminal-chip">${terminal.departureCount} departures</span>
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
    statusBanner.textContent = `Unable to load live flight activity: ${error.message}`;
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
