/*
 * 轟炸超人「同桌連線」：WebRTC 直連＋配對。
 * - 開房那支手機當主機，每個客人跟主機各一條 RTCPeerConnection、兩條 DataChannel：
 *   "r"（有序可靠：大廳、開局、結算）、"u"（無序不重送：快照、輸入）。
 * - 配對（交換 SDP）只靠廣播：正式版用 Supabase Realtime broadcast（方舟的專案、anon key），
 *   測試用 ?net=local → 同一個瀏覽器的 BroadcastChannel（multi.html 一頁開好幾個遊戲）。
 *   連上之後遊戲資料全部走 WebRTC，不經過 Supabase。
 * - 不等 trickle ICE：收集完候選（最多 2.5 秒）才把整包 SDP 送出去，訊息少、好除錯。
 * Unity 端（Assets/Plugins/WebGL/BomberNet.jslib）呼叫 window.BomberNet 的 start／send／poll／close。
 * 事件型別跟 C# 的 NetEventType 一樣：0 連上、1 斷線、2 資料、3 錯誤、4 狀態說明（給客人看「正在等什麼」）。
 * 10-22 豆豆點 LINE 邀請連結進不來：房主切到 LINE 時瀏覽器把遊戲頁暫停、配對的 websocket 被切斷又不會重連，
 * 客人只等 12 秒就放棄。現在：websocket 斷了自己重連、切回畫面立刻重連；客人等房主最多 60 秒、一路說明在等什麼。
 */
(function () {
  "use strict";
  var CFG = Object.assign({
    supabaseUrl: "https://xokrbgeldwsiywkugfkx.supabase.co",
    // 方舟 App 用的同一把公開 anon key（island_flutter/lib/net/ark_cloud.dart）；只拿來在 realtime 頻道交換連線資訊
    anonKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inhva3JiZ2VsZHdzaXl3a3VnZmt4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODc4MDIwNzAsImV4cCI6MjEwMzM3ODA3MH0.RiweNtahU8a5ZOZiq2kvpsuJwKLDdDed4PASIG1wfXU",
    stun: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"],
    iceWaitMs: 2500,
    joinTimeoutMs: 60000,
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

  /**
   * Supabase Realtime（Phoenix websocket 協定）最小實作：加入頻道、廣播 "sig"、25 秒心跳。
   * 斷線（手機切到別的 App、網路換手、伺服器踢掉）就自己重連（1、2、4…最多 10 秒）；畫面切回來、網路恢復時馬上重連。
   * 每次重新加入頻道成功都呼叫 onReady（房主用來重新喊一次「我在這」）。連續失敗 6 次才報錯。
   */
  function SupaSig(room, onMsg, onError, onReady) {
    var url = CFG.supabaseUrl.replace(/^http/, "ws") + "/realtime/v1/websocket?apikey=" + encodeURIComponent(CFG.anonKey) + "&vsn=1.0.0";
    var topic = "realtime:bomber:" + room;
    var ref = 1, joinRef = "1", joined = false, closed = false, queue = [], hb = null, ws = null, fails = 0, retry = null;
    function raw(o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
    function connect() {
      clearTimeout(retry);
      clearInterval(hb);
      joined = false;
      joinRef = String(++ref);
      var me = ws = new WebSocket(url);
      me.onopen = function () {
        if (me !== ws) return;
        raw({ topic: topic, event: "phx_join", ref: joinRef, join_ref: joinRef,
          payload: { config: { broadcast: { ack: false, self: false }, presence: { key: "" }, postgres_changes: [], private: false }, access_token: CFG.anonKey } });
        hb = setInterval(function () { raw({ topic: "phoenix", event: "heartbeat", payload: {}, ref: String(++ref) }); }, 25000);
      };
      me.onmessage = function (e) {
        if (me !== ws) return;
        var m; try { m = JSON.parse(e.data); } catch (x) { return; }
        if (m.topic !== topic) return;
        if (m.event === "phx_reply" && m.ref === joinRef) {
          if (m.payload && m.payload.status === "ok") {
            joined = true; fails = 0;
            log("realtime joined", topic);
            queue.splice(0).forEach(send);
            if (onReady) onReady();
          } else onError("配對伺服器拒絕連線");
        } else if (m.event === "broadcast" && m.payload && m.payload.event === "sig") {
          onMsg(m.payload.payload);
        } else if (m.event === "phx_error" || m.event === "phx_close") {
          log("realtime channel closed, reconnect");
          try { me.close(); } catch (x) {}
        }
      };
      me.onerror = function () { log("ws error"); };
      me.onclose = function () {
        if (me !== ws || closed) return;
        clearInterval(hb);
        joined = false;
        fails++;
        log("ws closed, fails", fails);
        if (fails >= 6) { onError("連不上配對伺服器（網路有通嗎？）"); return; }
        retry = setTimeout(connect, Math.min(10000, 1000 * Math.pow(2, fails - 1)));
      };
    }
    // 切回這個畫面／網路恢復：連線斷了就馬上重連，沒斷就補一次心跳確認還活著
    function wake() {
      if (closed || document.visibilityState === "hidden") return;
      if (!ws || ws.readyState > 1) { fails = 0; log("wake: reconnect"); connect(); }
      else raw({ topic: "phoenix", event: "heartbeat", payload: {}, ref: String(++ref) });
    }
    document.addEventListener("visibilitychange", wake);
    window.addEventListener("online", wake);
    window.addEventListener("pageshow", wake);
    connect();
    function send(m) {
      if (!joined) { queue.push(m); if (queue.length > 40) queue.shift(); return; }
      raw({ topic: topic, event: "broadcast", ref: String(++ref), join_ref: joinRef, payload: { type: "broadcast", event: "sig", payload: m } });
    }
    return { send: send, close: function () {
      closed = true; clearTimeout(retry); clearInterval(hb);
      document.removeEventListener("visibilitychange", wake);
      window.removeEventListener("online", wake);
      window.removeEventListener("pageshow", wake);
      try { if (ws) ws.close(); } catch (x) {}
    } };
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

  /** 客人畫面上方的說明（C# 顯示在大廳的狀態列）。 */
  function status(msg) { push(4, 0, null, msg); }

  function clientAccept(m) {
    var peer = { id: m.from, num: 0, pc: newPc(), open: false };
    N.hostPeer = peer;
    N.byId.set(m.from, peer);
    N.peers.set(0, peer);
    clearInterval(N.joinTimer);
    clearTimeout(N.joinGiveUp);
    clearTimeout(N.waitHint);
    status(N.attempts > 1 ? "再試一次連線（第 " + N.attempts + " 次）…" : "找到房主了，正在連線…");
    peer.pc.ondatachannel = function (e) { wireChannel(peer, e.channel); };
    watchPc(peer);
    peer.timer = setTimeout(function () {
      if (peer.open || !N || N.hostPeer !== peer) return;
      var ice = peer.pc.iceConnectionState;
      log("connect timeout", N.attempts, "ice", ice);
      drop(peer);
      // 沒連上：換一個身分重新加入（主機那邊舊的那條 15 秒後自己丟掉），最多試 3 次
      if (N.attempts < 3) { N.hostPeer = null; N.myId = rid(); startJoin(true); }
      else push(3, 0, null, "連不上房主：兩支手機的網路沒辦法直接連（常見一支用 Wi-Fi、一支用 4G）。請連同一個 Wi-Fi，或請房主開手機熱點給大家連，再按一次加入。");
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

  /**
   * 客人：每 1.5 秒喊一次「我要加入」，直到收到房主的 offer。房主可能正在 LINE 裡傳邀請（遊戲頁被暫停），
   * 所以最多等 60 秒；6 秒還沒回應就說明「請房主回到遊戲畫面」。retry＝連線沒成功、換身分重來（不用再等那麼久）。
   */
  function startJoin(retry) {
    N.attempts = (N.attempts || 0) + 1;
    clearInterval(N.joinTimer);
    clearTimeout(N.joinGiveUp);
    clearTimeout(N.waitHint);
    var hello = function () { sig({ t: "join" }); };
    hello();
    N.joinTimer = setInterval(hello, 1500);
    if (!retry) {
      status("正在找房主…");
      N.waitHint = setTimeout(function () {
        if (N && !N.hostPeer) status("房主還沒回應：請房主回到遊戲畫面（傳完 LINE 要切回遊戲），這邊會一直等…");
      }, 6000);
    }
    N.joinGiveUp = setTimeout(function () {
      if (!N || N.hostPeer) return;
      clearInterval(N.joinTimer);
      push(3, 0, null, "等不到房主：房主要停在遊戲的房間畫面，房號也要一樣。請房主回到遊戲後，再按一次加入。");
    }, retry ? 20000 : CFG.joinTimeoutMs);
  }

  // ------------------------------------------------------------------ 對 Unity 的介面

  function start(isHost, room, mode) {
    close();
    N = { events: [], peers: new Map(), byId: new Map(), myId: rid(), room: room, host: !!isHost, nextNum: 1, since: Date.now(), sig: null, hostPeer: null };
    var onMsg = isHost ? hostOnSig : clientOnSig;
    var onErr = function (msg) { push(3, 0, null, msg); };
    var wantLocal = mode === "local" || !window.WebSocket || !CFG.supabaseUrl;
    // 房主每次（重新）連上配對頻道就喊一次「我在這」：切去 LINE 再回來時，等著的客人馬上找得到
    var onReady = function () { if (N && N.host) sig({ t: "host", since: N.since }); };
    try { N.sig = wantLocal ? LocalSig(room, function (m) { if (N) onMsg(m); }) : SupaSig(room, function (m) { if (N) onMsg(m); }, onErr, onReady); }
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
    clearTimeout(n.waitHint);
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
