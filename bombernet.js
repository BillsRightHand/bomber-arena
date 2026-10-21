/*
 * 轟炸超人「同桌連線」：WebRTC 直連＋配對。
 * - 開房那支手機當主機，每個客人跟主機各一條 RTCPeerConnection、兩條 DataChannel：
 *   "r"（有序可靠：大廳、開局、結算）、"u"（無序不重送：快照、輸入）。
 * - 配對（交換 SDP）只靠廣播：正式版用 Supabase Realtime broadcast（方舟的專案、anon key），
 *   測試用 ?net=local → 同一個瀏覽器的 BroadcastChannel（multi.html 一頁開好幾個遊戲）。
 *   連上之後遊戲資料全部走 WebRTC，不經過 Supabase。
 * - 不等 trickle ICE：收集完候選（最多 2.5 秒）才把整包 SDP 送出去，訊息少、好除錯。
 * Unity 端（Assets/Plugins/WebGL/BomberNet.jslib）呼叫 window.BomberNet 的 start／send／poll／close。
 * 事件型別跟 C# 的 NetEventType 一樣：0 連上、1 斷線、2 資料、3 錯誤。
 */
(function () {
  "use strict";
  var CFG = Object.assign({
    supabaseUrl: "https://xokrbgeldwsiywkugfkx.supabase.co",
    // 方舟 App 用的同一把公開 anon key（island_flutter/lib/net/ark_cloud.dart）；只拿來在 realtime 頻道交換連線資訊
    anonKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inhva3JiZ2VsZHdzaXl3a3VnZmt4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODc4MDIwNzAsImV4cCI6MjEwMzM3ODA3MH0.RiweNtahU8a5ZOZiq2kvpsuJwKLDdDed4PASIG1wfXU",
    stun: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"],
    iceWaitMs: 2500,
    joinTimeoutMs: 12000,
    connectTimeoutMs: 15000,
  }, window.BOMBER_NET || {});

  var N = null;
  // 網址加 netdebug=1 就把每一步印到 console（現場除錯用）
  if (/[?&]netdebug=1/.test(location.search)) window.BOMBER_NET_DEBUG = true;
  var log = function () { if (window.BOMBER_NET_DEBUG) console.log.apply(console, ["[BomberNet]", (performance.now() / 1000).toFixed(1)].concat([].slice.call(arguments))); };

  function rid() { return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4); }
  function push(type, peer, data, msg) { if (N) N.events.push({ type: type, peer: peer, data: data || null, msg: msg || "" }); }

  // ------------------------------------------------------------------ 配對頻道

  function LocalSig(room, onMsg) {
    var bc = new BroadcastChannel("bomber:" + room);
    bc.onmessage = function (e) { onMsg(e.data); };
    return { send: function (m) { bc.postMessage(m); }, close: function () { bc.close(); } };
  }

  /** Supabase Realtime（Phoenix websocket 協定）最小實作：加入頻道、廣播 "sig"、25 秒心跳。 */
  function SupaSig(room, onMsg, onError) {
    var url = CFG.supabaseUrl.replace(/^http/, "ws") + "/realtime/v1/websocket?apikey=" + encodeURIComponent(CFG.anonKey) + "&vsn=1.0.0";
    var topic = "realtime:bomber:" + room;
    var ref = 1, joinRef = "1", joined = false, closed = false, queue = [], hb = null;
    var ws = new WebSocket(url);
    function raw(o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
    ws.onopen = function () {
      raw({ topic: topic, event: "phx_join", ref: joinRef, join_ref: joinRef,
        payload: { config: { broadcast: { ack: false, self: false }, presence: { key: "" }, postgres_changes: [], private: false }, access_token: CFG.anonKey } });
      hb = setInterval(function () { raw({ topic: "phoenix", event: "heartbeat", payload: {}, ref: String(++ref) }); }, 25000);
    };
    ws.onmessage = function (e) {
      var m; try { m = JSON.parse(e.data); } catch (x) { return; }
      if (m.topic !== topic) return;
      if (m.event === "phx_reply" && m.ref === joinRef) {
        if (m.payload && m.payload.status === "ok") { joined = true; queue.splice(0).forEach(send); log("realtime joined", topic); }
        else onError("配對伺服器拒絕連線");
      } else if (m.event === "broadcast" && m.payload && m.payload.event === "sig") {
        onMsg(m.payload.payload);
      } else if (m.event === "phx_error" || m.event === "phx_close") {
        if (!closed) onError("配對伺服器斷線");
      }
    };
    ws.onerror = function () { if (!closed) onError("連不上配對伺服器（網路有通嗎？）"); };
    ws.onclose = function () { clearInterval(hb); };
    function send(m) {
      if (!joined) { queue.push(m); return; }
      raw({ topic: topic, event: "broadcast", ref: String(++ref), join_ref: joinRef, payload: { type: "broadcast", event: "sig", payload: m } });
    }
    return { send: send, close: function () { closed = true; clearInterval(hb); try { ws.close(); } catch (x) {} } };
  }

  function sig(obj) {
    if (!N || !N.sig) return;
    obj.from = N.myId;
    N.sigOut = (N.sigOut || 0) + 1;
    log("sig ->", obj.t, obj.to || "", obj.sdp ? obj.sdp.sdp.length + "B" : "");
    N.sig.send(obj);
  }

  // ------------------------------------------------------------------ WebRTC

  function waitIce(pc, ms) {
    return new Promise(function (resolve) {
      if (pc.iceGatheringState === "complete") { resolve(); return; }
      var t = setTimeout(resolve, ms);
      pc.addEventListener("icegatheringstatechange", function () {
        if (pc.iceGatheringState === "complete") { clearTimeout(t); resolve(); }
      });
    });
  }

  function newPc() { return new RTCPeerConnection({ iceServers: [{ urls: CFG.stun }] }); }

  function wireChannel(peer, ch) {
    ch.binaryType = "arraybuffer";
    if (ch.label === "r") peer.rel = ch; else peer.unrel = ch;
    ch.onopen = function () { checkOpen(peer); };
    ch.onclose = function () { log("channel close", peer.num, ch.label); drop(peer); };
    ch.onmessage = function (e) {
      if (typeof e.data === "string") return;
      push(2, peer.num, new Uint8Array(e.data));
    };
  }

  function checkOpen(peer) {
    if (peer.open || !peer.rel || !peer.unrel) return;
    if (peer.rel.readyState !== "open" || peer.unrel.readyState !== "open") return;
    peer.open = true;
    clearTimeout(peer.timer);
    log("peer open", peer.num, peer.id);
    push(0, peer.num);
  }

  function drop(peer) {
    if (peer.gone) return;
    peer.gone = true;
    clearTimeout(peer.timer);
    try { peer.pc.close(); } catch (x) {}
    if (N) { N.byId.delete(peer.id); N.peers.delete(peer.num); }
    if (peer.open) push(1, peer.num);
    log("peer gone", peer.num);
  }

  function watchPc(peer) {
    peer.pc.oniceconnectionstatechange = function () { log("ice", peer.num, peer.pc.iceConnectionState); };
    peer.pc.onconnectionstatechange = function () {
      var s = peer.pc.connectionState;
      log("conn", peer.num, s);
      if (s === "failed" || s === "closed") drop(peer);
      else if (s === "disconnected") setTimeout(function () { if (peer.pc.connectionState === "disconnected") drop(peer); }, 5000);
    };
  }

  // ------------------------------------------------------------------ 主機

  function hostOnSig(m) {
    if (!m || m.from === N.myId) return;
    N.sigIn = (N.sigIn || 0) + 1;
    log("sig <-", m.t, m.from);
    if (m.t === "join") {
      var old = N.byId.get(m.from);
      if (old && (old.open || !old.gone)) { if (old.offer) sig({ t: "offer", to: m.from, sdp: old.offer }); return; }
      hostCreatePeer(m.from);
    } else if (m.t === "answer" && m.to === N.myId) {
      var p = N.byId.get(m.from);
      if (p && !p.answered) { p.answered = true; p.pc.setRemoteDescription(m.sdp).catch(function (e) { log("answer fail", e); drop(p); }); }
    } else if (m.t === "host" && m.since < N.since) {
      push(3, 0, null, "這個房號已經有人開房了，請改按「加入」");
    } else if (m.t === "who") {
      sig({ t: "host", since: N.since });
    }
  }

  function hostCreatePeer(remoteId) {
    var peer = { id: remoteId, num: N.nextNum++, pc: newPc(), open: false };
    N.byId.set(remoteId, peer);
    N.peers.set(peer.num, peer);
    wireChannel(peer, peer.pc.createDataChannel("r", { ordered: true }));
    wireChannel(peer, peer.pc.createDataChannel("u", { ordered: false, maxRetransmits: 0 }));
    watchPc(peer);
    peer.timer = setTimeout(function () { if (!peer.open) drop(peer); }, CFG.connectTimeoutMs);
    peer.pc.createOffer()
      .then(function (o) { return peer.pc.setLocalDescription(o); })
      .then(function () { return waitIce(peer.pc, CFG.iceWaitMs); })
      .then(function () {
        peer.offer = { type: peer.pc.localDescription.type, sdp: peer.pc.localDescription.sdp };
        sig({ t: "offer", to: remoteId, sdp: peer.offer });
        log("offer sent", peer.num);
      })
      .catch(function (e) { log("offer fail", e); drop(peer); });
  }

  // ------------------------------------------------------------------ 客人

  function clientOnSig(m) {
    if (!m || m.from === N.myId) return;
    N.sigIn = (N.sigIn || 0) + 1;
    log("sig <-", m.t, m.to === N.myId ? "(to me)" : "");
    if (m.t === "offer" && m.to === N.myId && !N.hostPeer) clientAccept(m);
  }

  function clientAccept(m) {
    var peer = { id: m.from, num: 0, pc: newPc(), open: false };
    N.hostPeer = peer;
    N.byId.set(m.from, peer);
    N.peers.set(0, peer);
    clearInterval(N.joinTimer);
    clearTimeout(N.joinGiveUp);
    peer.pc.ondatachannel = function (e) { wireChannel(peer, e.channel); };
    watchPc(peer);
    peer.timer = setTimeout(function () {
      if (peer.open || !N || N.hostPeer !== peer) return;
      drop(peer);
      // 沒連上：換一個身分重新加入（主機那邊舊的那條 15 秒後自己丟掉），最多試 3 次
      if (N.attempts < 3) { log("connect timeout, retry", N.attempts); N.hostPeer = null; N.myId = rid(); startJoin(); }
      else push(3, 0, null, "連不上房主（大家連同一個 Wi-Fi 再試一次）");
    }, CFG.connectTimeoutMs);
    peer.pc.setRemoteDescription(m.sdp)
      .then(function () { return peer.pc.createAnswer(); })
      .then(function (a) { return peer.pc.setLocalDescription(a); })
      .then(function () { return waitIce(peer.pc, CFG.iceWaitMs); })
      .then(function () {
        sig({ t: "answer", to: m.from, sdp: { type: peer.pc.localDescription.type, sdp: peer.pc.localDescription.sdp } });
        log("answer sent");
      })
      .catch(function (e) { log("answer fail", e); push(3, 0, null, "連線失敗"); drop(peer); });
  }

  /** 客人：每 1.5 秒喊一次「我要加入」，直到收到房主的 offer；12 秒都沒回應就報錯。 */
  function startJoin() {
    N.attempts = (N.attempts || 0) + 1;
    clearInterval(N.joinTimer);
    clearTimeout(N.joinGiveUp);
    var hello = function () { sig({ t: "join" }); };
    hello();
    N.joinTimer = setInterval(hello, 1500);
    N.joinGiveUp = setTimeout(function () {
      if (!N || N.hostPeer) return;
      clearInterval(N.joinTimer);
      push(3, 0, null, "找不到這個房間（房號對嗎？房主開房了嗎？）");
    }, CFG.joinTimeoutMs);
  }

  // ------------------------------------------------------------------ 對 Unity 的介面

  function start(isHost, room, mode) {
    close();
    N = { events: [], peers: new Map(), byId: new Map(), myId: rid(), room: room, host: !!isHost, nextNum: 1, since: Date.now(), sig: null, hostPeer: null };
    var onMsg = isHost ? hostOnSig : clientOnSig;
    var onErr = function (msg) { push(3, 0, null, msg); };
    var wantLocal = mode === "local" || !window.WebSocket || !CFG.supabaseUrl;
    try { N.sig = wantLocal ? LocalSig(room, function (m) { if (N) onMsg(m); }) : SupaSig(room, function (m) { if (N) onMsg(m); }, onErr); }
    catch (e) { onErr("配對失敗：" + e); return 0; }
    if (typeof RTCPeerConnection === "undefined") { onErr("這個瀏覽器不支援連線對戰"); return 0; }
    if (isHost) {
      sig({ t: "host", since: N.since });
    } else {
      startJoin();
    }
    log("start", isHost ? "host" : "join", room, wantLocal ? "local" : "supabase", N.myId);
    return 1;
  }

  function send(peerNum, bytes, reliable) {
    if (!N) return;
    var targets = peerNum < 0 ? Array.from(N.peers.values()) : [N.peers.get(peerNum)];
    for (var i = 0; i < targets.length; i++) {
      var p = targets[i];
      if (!p || !p.open) continue;
      var ch = reliable ? p.rel : p.unrel;
      if (!ch || ch.readyState !== "open") continue;
      if (!reliable && ch.bufferedAmount > 256 * 1024) continue;   // 塞車就丟（下一包快照會補）
      try { ch.send(bytes); } catch (e) { log("send fail", e); }
    }
  }

  function poll() { return N && N.events.length ? N.events.shift() : null; }

  function disconnect(peerNum) {
    if (!N) return;
    var p = N.peers.get(peerNum);
    if (p) drop(p);
  }

  function close() {
    if (!N) return;
    var n = N;
    N = null;
    clearInterval(n.joinTimer);
    clearTimeout(n.joinGiveUp);
    n.peers.forEach(function (p) { p.gone = true; clearTimeout(p.timer); try { p.pc.close(); } catch (x) {} });
    if (n.sig) n.sig.close();
  }

  window.addEventListener("pagehide", close);
  window.BomberNet = {
    start: start, send: send, poll: poll, disconnect: disconnect, close: close,
    get state() {
      return N ? { host: N.host, room: N.room, sigIn: N.sigIn || 0, sigOut: N.sigOut || 0,
        peers: Array.from(N.peers.values()).map(function (p) {
          return { num: p.num, open: p.open, ice: p.pc.iceConnectionState, conn: p.pc.connectionState, sig: p.pc.signalingState,
            r: p.rel ? p.rel.readyState : "-", u: p.unrel ? p.unrel.readyState : "-" };
        }), queued: N.events.length } : null;
    },
  };
})();
