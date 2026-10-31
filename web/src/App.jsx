import { useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  ArrowDownRight,
  ArrowUpRight,
  Check,
  ChevronDown,
  CircleHelp,
  Clock3,
  Command,
  Gamepad2,
  Globe2,
  Radio,
  Search,
  Send,
  Server,
  Shield,
  Swords,
  Users,
  Wifi,
  WifiOff,
  Zap,
} from "lucide-react";

const regions = [
  { value: "na-west", label: "North America West", short: "NA-W" },
  { value: "na-east", label: "North America East", short: "NA-E" },
  { value: "eu-west", label: "Europe West", short: "EU-W" },
];

function makePlayerID() {
  return `player-${Math.random().toString(36).slice(2, 8)}`;
}

function averageMMR(players = []) {
  if (!players.length) return 0;
  return Math.round(players.reduce((total, player) => total + player.mmr, 0) / players.length);
}

function App() {
  const [connection, setConnection] = useState("connecting");
  const [matches, setMatches] = useState([]);
  const [playerID, setPlayerID] = useState(makePlayerID);
  const [mmr, setMMR] = useState(1200);
  const [region, setRegion] = useState("na-west");
  const [botCount, setBotCount] = useState(100);
  const [notice, setNotice] = useState(null);
  const [busy, setBusy] = useState("");
  const [metrics, setMetrics] = useState(null);
  const [metricsError, setMetricsError] = useState("");
  const [activeLobbyCount, setActiveLobbyCount] = useState(0);
  const [now, setNow] = useState(Date.now());
  const playerIDRef = useRef(playerID);
  playerIDRef.current = playerID.trim();

  useEffect(() => {
    let active = true;
    let controller;

    async function refreshMetrics() {
      controller?.abort();
      controller = new AbortController();
      try {
        const [metricsResponse, lobbiesResponse] = await Promise.all([
          fetch(`/api/metrics?region=${encodeURIComponent(region)}`, { signal: controller.signal }),
          fetch(`/api/lobbies?region=${encodeURIComponent(region)}`, { signal: controller.signal }),
        ]);
        if (!metricsResponse.ok) {
          throw new Error((await metricsResponse.text()).trim() || `Metrics request failed (${metricsResponse.status})`);
        }
        if (!lobbiesResponse.ok) {
          throw new Error((await lobbiesResponse.text()).trim() || `Lobby request failed (${lobbiesResponse.status})`);
        }
        const [nextMetrics, lobbies] = await Promise.all([
          metricsResponse.json(),
          lobbiesResponse.json(),
        ]);
        if (!active) return;
        setMetrics(nextMetrics);
        setActiveLobbyCount(lobbies.length);
        setMetricsError("");
        setMatches((current) => {
          const byID = new Map(current.map((match) => [match.id, match]));
          for (const lobby of lobbies) {
            const matchID = lobby.match_id || lobby.id;
            byID.set(matchID, {
              id: matchID,
              region: lobby.region,
              created_at: lobby.created_at,
              lobby_expires_at: lobby.expires_at,
              players: lobby.players,
              team_one: lobby.team_one,
              team_two: lobby.team_two,
            });
          }
          return [...byID.values()]
            .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
            .slice(0, 20);
        });
      } catch (error) {
        if (!active || error.name === "AbortError") return;
        setMetricsError(error.message || "Could not load queue metrics.");
      }
    }

    refreshMetrics();
    const interval = window.setInterval(refreshMetrics, 2000);
    return () => {
      active = false;
      controller?.abort();
      window.clearInterval(interval);
    };
  }, [region]);

  useEffect(() => {
    let active = true;
    let socket;
    let retryTimer;
    let retryDelay = 1000;

    function connect() {
      if (!active) return;
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(`${protocol}//${window.location.host}/ws`);
      setConnection("connecting");

      socket.onopen = () => {
        if (!active) return;
        retryDelay = 1000;
        setConnection("connected");
      };
      socket.onmessage = (event) => {
        if (!active) return;
        try {
          const match = JSON.parse(event.data);
          if (!match.id || !Array.isArray(match.players)) return;
          setMatches((current) => [match, ...current].slice(0, 20));
          if (match.players.some((player) => player.player_id === playerIDRef.current)) {
            setNotice({
              kind: "success",
              text: `You matched in lobby ${match.id}.`,
            });
          }
        } catch {
          setNotice({ kind: "error", text: "Received an unreadable match event." });
        }
      };
      socket.onerror = () => socket.close();
      socket.onclose = () => {
        if (!active) return;
        setConnection("reconnecting");
        retryTimer = window.setTimeout(connect, retryDelay);
        retryDelay = Math.min(retryDelay * 2, 15000);
      };
    }

    connect();
    return () => {
      active = false;
      window.clearTimeout(retryTimer);
      socket?.close();
    };
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(interval);
  }, []);

  const selectedRegion = regions.find((item) => item.value === region);
  const matchRate = useMemo(() => matches.length, [matches]);
  const maxBucketCount = Math.max(1, ...(metrics?.mmr_distribution || []).map((bucket) => bucket.count));

  async function sendRequest(path, body, action) {
    setBusy(action);
    setNotice(null);
    try {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const responseBody = await response.text();
      if (!response.ok) {
        throw new Error(responseBody.trim() || `Request failed (${response.status})`);
      }
      const result = responseBody ? JSON.parse(responseBody) : {};
      if (action === "join") {
        setNotice({ kind: "success", text: `${result.player_id} joined ${region}.` });
      } else {
        setNotice({ kind: "success", text: `${result.accepted} bot tickets added to ${region}.` });
      }
    } catch (error) {
      setNotice({ kind: "error", text: error.message || "Request failed." });
    } finally {
      setBusy("");
    }
  }

  function joinQueue(event) {
    event.preventDefault();
    const cleanID = playerID.trim();
    if (!cleanID) {
      setNotice({ kind: "error", text: "Enter a player ID before joining." });
      return;
    }
    sendRequest(
      "/api/queue/join",
      { player_id: cleanID, mmr: Number(mmr), region },
      "join",
    );
  }

  function launchBots() {
    sendRequest(
      "/api/queue/simulate",
      { count: botCount, region, mean_mmr: Number(mmr), stddev_mmr: 100 },
      "bots",
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="#" aria-label="Matchmaker home">
          <span className="brand-mark"><Swords size={19} /></span>
          <span>match<span className="brand-light">maker</span></span>
          <span className="brand-version">LAB</span>
        </a>

        <div className="workspace-label">WORKSPACE</div>
        <button className="workspace-switcher">
          <span className="workspace-icon"><Command size={16} /></span>
          <span className="workspace-copy"><strong>Match Ops</strong><small>Personal workspace</small></span>
          <ChevronDown size={15} className="muted-icon" />
        </button>

        <div className="nav-label">MONITORING</div>
        <a className="nav-item active" href="#overview"><Activity size={17} /> Overview <span className="nav-pip" /></a>
        <a className="nav-item" href="#matches"><Swords size={17} /> Match feed <span className="nav-count">{matches.length}</span></a>
        <a className="nav-item" href="#queue-controls"><Users size={17} /> Queue controls</a>

        <div className="sidebar-bottom">
          <div className="environment-card">
            <div className="environment-head"><span className="tiny-dot" /> ENVIRONMENT</div>
            <strong>Development</strong>
            <span>Local service · Upstash Redis</span>
          </div>
          <a className="nav-item help-link" href="#about"><CircleHelp size={17} /> How it works</a>
          <div className="profile">
            <div className="avatar">B</div>
            <div><strong>Builder</strong><small>Portfolio project</small></div>
            <button aria-label="Profile options" className="icon-button"><ChevronDown size={15} /></button>
          </div>
        </div>
      </aside>

      <main className="main-content" id="overview">
        <header className="topbar">
          <div className="breadcrumbs"><span>Match Ops</span><span className="crumb-slash">/</span><strong>Overview</strong></div>
          <div className="topbar-right">
            <div className={`connection-pill ${connection}`}>
              <span className="connection-dot" />
              {connection === "connected" ? "LIVE CONNECTION" : connection === "connecting" ? "CONNECTING" : "RECONNECTING"}
            </div>
            <span className="topbar-divider" />
            <button className="icon-button search-button" aria-label="Search"><Search size={17} /></button>
            <span className="avatar top-avatar">B</span>
          </div>
        </header>

        <div className="page-wrap">
          <section className="page-heading">
            <div>
              <div className="eyebrow"><span className="eyebrow-line" /> REAL-TIME OPERATIONS</div>
              <h1>Matchmaking <span>overview</span></h1>
              <p>Monitor queues, run simulations, and watch matches form live.</p>
            </div>
            <div className="heading-meta"><span className="live-wave"><Radio size={15} /></span> Streaming events <span className="meta-separator">·</span> WebSocket</div>
          </section>

          <section className="stat-grid" aria-label="Live service summary">
            <article className="stat-card">
              <div className="stat-top"><span className="stat-label">ACTIVE LOBBIES</span><span className="stat-icon violet"><Swords size={17} /></span></div>
              <div className="stat-value">{activeLobbyCount.toString().padStart(2, "0")}<span className="stat-unit"> in {selectedRegion?.short}</span></div>
              <div className="stat-foot"><span className="foot-neutral"><Clock3 size={13} /> 5-minute lifetime</span><span>{matchRate} feed events</span></div>
            </article>
            <article className="stat-card">
              <div className="stat-top"><span className="stat-label">SOCKET STATUS</span><span className={`stat-icon ${connection === "connected" ? "green" : "amber"}`}><Wifi size={17} /></span></div>
              <div className="stat-value status-value">{connection === "connected" ? "Online" : connection === "connecting" ? "Connecting" : "Retrying"}</div>
              <div className="stat-foot"><span className={connection === "connected" ? "foot-good" : "foot-neutral"}>{connection === "connected" ? <Check size={13} /> : <WifiOff size={13} />}{connection === "connected" ? " Receiving events" : " Waiting for server"}</span><span>Auto-reconnect enabled</span></div>
            </article>
            <article className="stat-card">
              <div className="stat-top"><span className="stat-label">ACTIVE QUEUE</span><span className="stat-icon blue"><Users size={17} /></span></div>
              <div className="stat-value">{metrics ? metrics.queue_size.toLocaleString() : "—"}<span className="stat-unit"> players</span></div>
              <div className="stat-foot"><span className={metricsError ? "foot-error" : "foot-neutral"}>{metricsError ? <CircleHelp size={13} /> : <Activity size={13} />}{metricsError ? " Metrics unavailable" : " Live Redis snapshot"}</span><span>{metrics?.updated_at ? new Date(metrics.updated_at).toLocaleTimeString() : ""}</span></div>
            </article>
            <article className="stat-card">
              <div className="stat-top"><span className="stat-label">AVG. WAIT TIME</span><span className="stat-icon orange"><Clock3 size={17} /></span></div>
              <div className="stat-value">{metrics ? `${(metrics.average_wait_ms / 1000).toFixed(1)}s` : "—"}</div>
              <div className="stat-foot"><span className="foot-neutral"><Clock3 size={13} /> For queued players</span><span>{selectedRegion?.short}</span></div>
            </article>
          </section>

          <section className="work-grid">
            <div className="left-column">
              <article className="panel queue-panel" id="queue-controls">
                <div className="panel-heading">
                  <div className="panel-title-wrap">
                    <span className="panel-icon"><Gamepad2 size={17} /></span>
                    <div><h2>Queue controls</h2><p>Join as a player or generate a load burst.</p></div>
                  </div>
                  <span className="panel-tag"><span className="tiny-dot" /> READY</span>
                </div>
                <form onSubmit={joinQueue}>
                  <div className="field-row">
                    <label className="field field-grow">
                      <span>PLAYER ID</span>
                      <input value={playerID} onChange={(event) => setPlayerID(event.target.value)} maxLength={64} placeholder="Enter a player ID" />
                    </label>
                    <label className="field field-mmr">
                      <span>SKILL RATING</span>
                      <div className="input-suffix"><input type="number" min="0" max="5000" value={mmr} onChange={(event) => setMMR(event.target.value)} /><span>MMR</span></div>
                    </label>
                  </div>
                  <label className="field region-field">
                    <span>REGION</span>
                    <div className="select-wrap"><Globe2 size={15} /><select value={region} onChange={(event) => setRegion(event.target.value)}>{regions.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select><ChevronDown size={15} /></div>
                  </label>
                  <button className="primary-button join-button" disabled={busy !== "" || connection !== "connected"} type="submit">
                    <Send size={15} /> {busy === "join" ? "Joining queue..." : "Join matchmaking queue"} <ArrowUpRight size={15} className="button-end" />
                  </button>
                </form>

                <div className="section-rule"><span>LOAD SIMULATION</span></div>
                <div className="bot-header"><div><strong>Spawn bot burst</strong><p>Generate synthetic players around your selected MMR.</p></div><span className="bot-count">{botCount}<small> bots</small></span></div>
                <div className="slider-wrap">
                  <input aria-label="Number of bots" type="range" min="10" max="500" step="10" value={botCount} onChange={(event) => setBotCount(Number(event.target.value))} style={{ "--range-progress": `${((botCount - 10) / 490) * 100}%` }} />
                  <div className="range-labels"><span>10</span><span>250</span><span>500 bots</span></div>
                </div>
                <button className="secondary-button" disabled={busy !== "" || connection !== "connected"} onClick={launchBots}>
                  <Zap size={15} /> {busy === "bots" ? "Sending tickets..." : `Spawn ${botCount} bots`} <ArrowDownRight size={15} className="button-end" />
                </button>
                {notice && <div className={`notice ${notice.kind}`} role="status">{notice.kind === "success" ? <Check size={15} /> : <CircleHelp size={15} />}{notice.text}</div>}
                <div className="control-foot"><Shield size={13} /> Simulated bots expire after 5 minutes; real player tickets remain queued until matched.</div>
              </article>

              <article className="panel distribution-panel">
                <div className="panel-heading compact-heading">
                  <div className="panel-title-wrap">
                    <span className="panel-icon chart-icon"><Activity size={17} /></span>
                    <div><h2>Queue distribution</h2><p>MMR population across the selected region.</p></div>
                  </div>
                  <label className="mini-select"><span className="sr-only">Distribution region</span><select value={region} onChange={(event) => setRegion(event.target.value)}>{regions.map((item) => <option key={item.value} value={item.value}>{item.short}</option>)}</select><ChevronDown size={13} /></label>
                </div>
                <div className="chart-placeholder">
                  <div className="chart-grid-lines"><span>{maxBucketCount.toLocaleString()}</span><span>{Math.round(maxBucketCount * 0.75).toLocaleString()}</span><span>{Math.round(maxBucketCount * 0.5).toLocaleString()}</span><span>{Math.round(maxBucketCount * 0.25).toLocaleString()}</span><span>0</span></div>
                  <div className="chart-guides" />
                  {metrics?.queue_size > 0 && metrics?.mmr_distribution?.length ? (
                    <div className="chart-bars" role="img" aria-label={`MMR distribution for ${selectedRegion?.label}`}>
                      {metrics.mmr_distribution.map((bucket) => (
                        <div className="chart-bar-column" key={bucket.min} title={`${bucket.min.toLocaleString()}–${bucket.max.toLocaleString()} MMR: ${bucket.count} players`}>
                          <div className="chart-bar-track"><div className="chart-bar" style={{ height: `${bucket.count ? Math.max(4, (bucket.count / maxBucketCount) * 100) : 0}%` }} /></div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="chart-empty">
                      <div className="chart-empty-icon"><Activity size={20} /></div>
                      <strong>{metricsError ? "Metrics unavailable" : metrics ? "No players in this queue" : "Loading queue metrics"}</strong>
                      <span>{metricsError || (metrics ? "Queue players to populate the MMR distribution." : "Fetching the latest Redis snapshot.")}</span>
                    </div>
                  )}
                  <div className="chart-x-labels"><span>0</span><span>1k</span><span>2k</span><span>3k</span><span>4k</span><span>5k</span></div>
                </div>
                <div className="chart-footnote"><span><span className="legend-dot" /> Queue population</span><span>{metrics?.updated_at ? `Updated ${new Date(metrics.updated_at).toLocaleTimeString()}` : "Refreshing every 2 seconds"}</span></div>
              </article>
            </div>

            <aside className="right-column">
              <article className="panel feed-panel" id="matches">
                <div className="panel-heading feed-heading">
                  <div className="panel-title-wrap">
                    <span className="panel-icon match-icon"><Swords size={17} /></span>
                    <div><h2>Live match feed</h2><p>Recently assembled lobbies.</p></div>
                  </div>
                  <span className="feed-live"><span className="tiny-dot" /> LIVE</span>
                </div>
                <div className="feed-summary"><span><span className="summary-dot" /> {matches.length} {matches.length === 1 ? "match" : "matches"} this session</span><span>Newest first</span></div>
                {matches.length === 0 ? (
                  <div className="feed-empty">
                    <div className="empty-emblem"><Swords size={22} /></div>
                    <strong>No matches yet</strong>
                    <p>Queue players or spawn a bot burst. New lobbies will appear here in real time.</p>
                    <a href="#queue-controls">Open queue controls <ArrowDownRight size={13} /></a>
                  </div>
                ) : (
                  <div className="match-list">
                    {matches.map((match) => (
                      <MatchCard key={match.id} match={match} now={now} />
                    ))}
                  </div>
                )}
                <div className="feed-bottom"><Radio size={14} /> Listening for match events</div>
              </article>
              <article className="panel service-panel" id="about">
                <div className="service-heading"><span className="service-icon"><Server size={16} /></span><div><h3>System connection</h3><span>Local development stack</span></div><span className={`service-status ${connection}`}><i />{connection === "connected" ? "HEALTHY" : "PENDING"}</span></div>
                <div className="service-row"><span>API gateway</span><span><i className="service-dot good" /> localhost:8080</span></div>
                <div className="service-row"><span>Event transport</span><span><i className={`service-dot ${connection === "connected" ? "good" : "waiting"}`} /> WebSocket</span></div>
                <div className="service-row"><span>Queue telemetry</span><span><i className={`service-dot ${metricsError ? "waiting" : "good"}`} /> {metricsError ? "Unavailable" : "2s Redis snapshots"}</span></div>
                <div className="service-note"><CircleHelp size={13} /> Lobbies expire after five minutes; match events remain in history.</div>
              </article>
            </aside>
          </section>

          <footer className="page-footer"><span>Matchmaker <span className="footer-sep">/</span> Live operations console</span><span><span className="footer-dot" /> Backend events are delivered via WebSocket</span></footer>
        </div>
      </main>
    </div>
  );
}

function MatchCard({ match, now }) {
  const teamOne = match.team_one || [];
  const teamTwo = match.team_two || [];
  const regionLabel = regions.find((item) => item.value === match.region)?.short || match.region;
  const created = match.created_at ? new Date(match.created_at) : null;
  const timeLabel = created && !Number.isNaN(created.getTime())
    ? created.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
    : "Just now";
  const expiresAt = new Date(match.lobby_expires_at).getTime();
  const remainingMs = Number.isFinite(expiresAt) ? expiresAt - now : 0;
  const lobbyStatus = remainingMs > 0
    ? `ACTIVE · ${Math.floor(remainingMs / 60000).toString().padStart(2, "0")}:${Math.floor((remainingMs % 60000) / 1000).toString().padStart(2, "0")}`
    : "EXPIRED";

  return (
    <article className="match-card">
      <div className="match-card-top"><span className="lobby-id"><span className={`lobby-live-dot ${remainingMs <= 0 ? "expired" : ""}`} /> LOBBY <strong>{match.id}</strong></span><span className={`lobby-status ${remainingMs > 0 ? "active" : "expired"}`}>{lobbyStatus}</span></div>
      <div className="match-time">{timeLabel}</div>
      <div className="match-meta"><span><Globe2 size={12} /> {regionLabel}</span><span><Users size={12} /> {match.players.length} players</span></div>
      <div className="teams">
        <div className="team-row">
          <span className="team-color team-blue" /><span className="team-name">TEAM 01</span><span className="team-mmr">{averageMMR(teamOne)} <small>AVG</small></span>
        </div>
        <div className="team-players">{teamOne.map((player) => <span key={player.player_id} title={`${player.player_id} · ${player.mmr} MMR`}>{player.player_id}</span>)}</div>
        <div className="team-row team-two">
          <span className="team-color team-orange" /><span className="team-name">TEAM 02</span><span className="team-mmr">{averageMMR(teamTwo)} <small>AVG</small></span>
        </div>
        <div className="team-players">{teamTwo.map((player) => <span key={player.player_id} title={`${player.player_id} · ${player.mmr} MMR`}>{player.player_id}</span>)}</div>
      </div>
    </article>
  );
}

export default App;
