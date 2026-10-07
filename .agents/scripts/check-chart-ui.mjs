import { spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";

const origin = process.env.KAFKA_QA_ORIGIN || "http://localhost:3000";
const artifacts = fileURLToPath(new URL("../review/artifacts/chart-live/", import.meta.url));
const profile = await mkdtemp(join(tmpdir(), "kafkalab-chart-qa-"));
await mkdir(artifacts, { recursive: true });
const chrome = spawn(process.env.KAFKA_QA_CHROME || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", [
  "--headless=new", "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
  "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", `--user-data-dir=${profile}`, "about:blank",
], { stdio: "ignore" });
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const evidence = { origin, charts: {}, errors: [] };
let socket;
let captureFailure;
try {
  let port;
  for (let attempt = 0; attempt < 60; attempt++) {
    try { port = (await readFile(join(profile, "DevToolsActivePort"), "utf8")).split("\n")[0]; break; } catch { await delay(100); }
  }
  assert(port, "isolated Chrome is available");
  const targets = await (await fetch(`http://localhost:${port}/json/list`)).json();
  socket = new WebSocket(targets.find((target) => target.type === "page").webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { socket.addEventListener("open", resolve, { once: true }); socket.addEventListener("error", reject, { once: true }); });
  let serial = 0;
  const pending = new Map();
  socket.addEventListener("message", ({ data }) => {
    const message = JSON.parse(data);
    if (message.id) { const item = pending.get(message.id); if (item) { pending.delete(message.id); message.error ? item.reject(new Error(JSON.stringify(message.error))) : item.resolve(message.result); } }
    if (message.method === "Runtime.exceptionThrown") evidence.errors.push(message.params.exceptionDetails.exception?.description || message.params.exceptionDetails.text);
    if (message.method === "Runtime.consoleAPICalled" && message.params.type === "error") evidence.errors.push(message.params.args.map((arg) => arg.value || arg.description).join(" "));
  });
  const cdp = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++serial;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)); }, 15000);
    pending.set(id, { resolve: (value) => { clearTimeout(timer); resolve(value); }, reject: (error) => { clearTimeout(timer); reject(error); } });
    socket.send(JSON.stringify({ id, method, params }));
  });
  const evaluate = async (expression) => {
    const result = await cdp("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description || result.exceptionDetails.text);
    return result.result.value;
  };
  const waitFor = async (expression) => {
    for (let attempt = 0; attempt < 150; attempt++) { if (await evaluate(`Boolean(${expression})`)) return; await delay(100); }
    throw new Error(`Timed out: ${expression}`);
  };
  const navigate = async (path) => {
    await cdp("Page.navigate", { url: `${origin}${path}` });
    await waitFor("document.readyState === 'complete' && document.querySelector('#main-content') && !document.querySelector('.kl-app-loading')");
  };
  const click = async (query) => {
    await evaluate(`(${query}).scrollIntoView({behavior:'instant',block:'nearest',inline:'nearest'})`);
    const point = await evaluate(`(()=>{const r=(${query}).getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    const hit = await evaluate(`document.elementFromPoint(${point.x},${point.y})?.tagName`);
    (evidence.clicks ??= []).push({ query, point, hit });
    await cdp("Input.dispatchMouseEvent", { type: "mouseMoved", ...point });
    await cdp("Input.dispatchMouseEvent", { type: "mousePressed", button: "left", clickCount: 1, ...point });
    await cdp("Input.dispatchMouseEvent", { type: "mouseReleased", button: "left", clickCount: 1, ...point });
  };
  const button = (text) => `[...document.querySelectorAll('button')].find(e=>e.textContent.trim()===${JSON.stringify(text)})`;
  const screenshot = async (name) => {
    const shot = await cdp("Page.captureScreenshot", { format: "png" });
    await writeFile(join(artifacts, `${name}.png`), Buffer.from(shot.data, "base64"));
  };
  captureFailure = () => screenshot("failure");
  const tab = (variant) => `document.getElementById('chart-tab-${variant}')`;
  const variants = ["flow", "partitions", "consumers", "offsets", "delivery", "replication", "activity"];
  const dragRelationships = {
    flow: ["producer-checkout-api", "producer-checkout-api-topic"], partitions: ["topic", "topic-partition-0"],
    consumers: ["group", "group-consumer-consumer-0"], offsets: ["offset-model", "offset-model-0"],
    delivery: ["delivery-queued", "delivery-queued-in-flight"], replication: ["topic", "replication-topic-0"],
    activity: ["activity-root", "activity-root-produced"],
  };
  await cdp("Page.enable"); await cdp("Runtime.enable");
  await cdp("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await navigate("/");
  await waitFor("document.querySelectorAll('.react-flow__node').length > 0");
  assert.equal(await evaluate("document.querySelectorAll('.kl-flow-token').length"), 0, "seed history does not animate");

  for (const variant of variants) {
    await click(tab(variant));
    await waitFor(`document.getElementById('chart-tab-${variant}').getAttribute('aria-selected')==='true' && document.querySelectorAll('.react-flow__edge-path').length>0`);
    await click(button("Run flow"));
    await waitFor("document.querySelector('.kl-flow-token[data-transit-state=active]')");
    const [dragNode, dragEdge] = dragRelationships[variant];
    const dragQuery = `document.querySelector('.react-flow__node[data-id="${dragNode}"] .kl-flow-node')`;
    const beforeDrag = await evaluate(`(()=>{const r=(${dragQuery}).getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2,path:document.getElementById(${JSON.stringify(dragEdge)}).getAttribute('d')}})()`);
    await cdp("Input.dispatchMouseEvent", { type: "mouseMoved", x: beforeDrag.x, y: beforeDrag.y });
    await cdp("Input.dispatchMouseEvent", { type: "mousePressed", button: "left", buttons: 1, clickCount: 1, x: beforeDrag.x, y: beforeDrag.y });
    for (let step = 1; step <= 5; step++) await cdp("Input.dispatchMouseEvent", { type: "mouseMoved", button: "left", buttons: 1, x: beforeDrag.x + step * 8, y: beforeDrag.y + step * 4 });
    await cdp("Input.dispatchMouseEvent", { type: "mouseReleased", button: "left", buttons: 0, clickCount: 1, x: beforeDrag.x + 40, y: beforeDrag.y + 20 });
    await waitFor(`document.getElementById(${JSON.stringify(dragEdge)}).getAttribute('d')!==${JSON.stringify(beforeDrag.path)}`);
    // Moving the card must update its real connection while live events continue.
    const afterDrag = await evaluate(`(()=>{const r=(${dragQuery}).getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    assert(Math.abs(afterDrag.x - beforeDrag.x) > 15, `${variant}: whole-card dragging works`);
    const first = await evaluate("(()=>{const e=[...document.querySelectorAll('.kl-flow-token[data-transit-state=active]')].at(-1); const m=e.getCTM(); return {id:e.dataset.tokenId,edge:e.dataset.edgeId,x:m.e,y:m.f}})()");
    await delay(80);
    const second = await evaluate(`(()=>{const e=[...document.querySelectorAll('.kl-flow-token')].find(e=>e.dataset.tokenId===${JSON.stringify(first.id)});const m=e?.getCTM();return e?{id:e.dataset.tokenId,x:m.e,y:m.f}:null})()`);
    assert(second && (Math.abs(first.x - second.x) > 0.5 || Math.abs(first.y - second.y) > 0.5), `${variant}: same packet moves on the actual edge`);
    assert(await evaluate("document.querySelectorAll('.kl-flow-token').length <= 24"), `${variant}: bounded tokens`);
    await screenshot(`${variant}-live`);
    await click(button("Pause flow"));
    await waitFor("document.querySelectorAll('.kl-flow-token').length===0 && document.querySelectorAll('.kl-flow-edge--active').length===0");
    assert.equal(await evaluate("document.querySelectorAll('.kl-flow-node--active').length"), 0, `${variant}: paused historical counts do not pulse`);
    const node = variant === "activity" ? "activity-root" : variant === "consumers" ? "group" : variant === "delivery" ? "delivery-in-flight" : "partition-0";
    const query = `document.querySelector('.react-flow__node[data-id="${node}"] .kl-flow-node')`;
    const point = await evaluate(`(()=>{const r=(${query}).getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    await cdp("Input.dispatchMouseEvent", { type: "mouseMoved", ...point });
    await waitFor("document.querySelector('[role=tooltip]')");
    const tooltip = await evaluate("(()=>{const e=document.querySelector('[role=tooltip]'),r=e.getBoundingClientRect(); return {parent:e.parentElement.tagName,width:r.width,left:r.left,right:r.right,top:r.top,bottom:r.bottom,scroll:e.scrollHeight,height:e.clientHeight}})()");
    assert.equal(tooltip.parent, "BODY", `${variant}: tooltip escapes graph transforms`);
    assert(tooltip.width >= 300 && tooltip.left >= 0 && tooltip.right <= 1440 && tooltip.top >= 0 && tooltip.bottom <= 1000, `${variant}: tooltip fits the viewport without graph scaling`);
    await screenshot(`${variant}-tooltip`);
    await cdp("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape" });
    await cdp("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape" });
    await waitFor("!document.querySelector('[role=tooltip]')");
    evidence.charts[variant] = { first, second, pauseCleanup: true, tooltip, drag: { before: beforeDrag, after: afterDrag, pathUpdated: true } };
  }

  await click(tab("offsets"));
  const offsetOverlap = await evaluate("(()=>{const cards=[...document.querySelectorAll('.react-flow__node[data-id^=offset-committed],.react-flow__node[data-id^=offset-current],.react-flow__node[data-id^=offset-end]')].map(e=>({id:e.dataset.id,r:e.getBoundingClientRect()}));return cards.some((a,i)=>cards.slice(i+1).some(b=>a.r.left<b.r.right&&a.r.right>b.r.left&&a.r.top<b.r.bottom&&a.r.bottom>b.r.top))})()");
  assert.equal(offsetOverlap, false, "offset cards do not overlap");
  evidence.offsetOverlap = false;

  for (const width of [390, 320]) {
    await cdp("Emulation.setDeviceMetricsOverride", { width, height: 844, deviceScaleFactor: 1, mobile: true });
    for (const variant of variants) {
      await click(tab(variant));
      await waitFor(`document.getElementById('chart-tab-${variant}').getAttribute('aria-selected')==='true' && document.querySelectorAll('.react-flow__node').length>0`);
      assert(await evaluate(`document.documentElement.scrollWidth <= ${width}`), `${variant}: no page overflow at ${width}`);
    }
    await click(tab("flow"));
    await waitFor("document.getElementById('chart-tab-flow').getAttribute('aria-selected')==='true' && document.querySelector('.react-flow__node[data-id=partition-0]')");
    const query = "document.querySelector('.react-flow__node[data-id=partition-0]')";
    await waitFor(`(${query}) && getComputedStyle(${query}).visibility==='visible' && (${query}).getBoundingClientRect().width>0`);
    await evaluate(`(${query}).focus()`);
    evidence[`focus${width}`] = await evaluate(`(()=>{const e=${query};return {active:document.activeElement?.outerHTML.slice(0,350),wrapper:e.outerHTML.slice(0,350),frameTabIndex:e.querySelector('.kl-flow-node')?.tabIndex,hasFocus:e.matches(':focus-within')}})()`);
    await waitFor("document.querySelector('[role=tooltip]')");
    const geometry = await evaluate("(()=>{const r=document.querySelector('[role=tooltip]').getBoundingClientRect();return {left:r.left,right:r.right,top:r.top,bottom:r.bottom}})()");
    assert(geometry.left >= 0 && geometry.right <= width && geometry.top >= 0 && geometry.bottom <= 844, `tooltip fits ${width}`);
    await screenshot(`mobile-${width}`);
    await cdp("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape" });
    await cdp("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape" });
    evidence[`mobile${width}`] = { noOverflow: true, tooltip: geometry };
  }

  await cdp("Emulation.setDeviceMetricsOverride", { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await click("document.querySelector('.kl-theme-toggle')");
  assert.equal(await evaluate("document.documentElement.dataset.theme"), "dark");
  await screenshot("dark-flow");
  for (const variant of variants) {
    await click(tab(variant));
    await waitFor(`document.getElementById('chart-tab-${variant}').getAttribute('aria-selected')==='true' && document.querySelectorAll('.react-flow__node').length>0`);
    await waitFor("getComputedStyle(document.querySelector('.react-flow__node')).visibility==='visible'");
    await evaluate("document.querySelector('.react-flow__node').focus()");
    await waitFor("document.querySelector('[role=tooltip]')");
    const colors = await evaluate("(()=>{const s=getComputedStyle(document.querySelector('[role=tooltip]'));return {background:s.backgroundColor,text:s.color}})()");
    assert.notEqual(colors.background, "rgb(255, 255, 255)", `${variant}: dark tooltip surface`);
    await cdp("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape" });
    await cdp("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape" });
    evidence.charts[variant].dark = colors;
  }
  await cdp("Emulation.setEmulatedMedia", { features: [{ name: "prefers-reduced-motion", value: "reduce" }] });
  await click(tab("activity")); await click(button("Run flow"));
  await waitFor("document.querySelector('.kl-flow-token')");
  assert.equal(await evaluate("document.querySelectorAll('.kl-flow-edge--animated').length"), 0, "reduced motion has no animated connections");
  const fixed = await evaluate("(()=>{const e=document.querySelector('.kl-flow-token');return {id:e.dataset.tokenId,transform:e.getAttribute('transform')}})()");
  await delay(100);
  assert.equal(await evaluate(`([...document.querySelectorAll('.kl-flow-token')].find(e=>e.dataset.tokenId===${JSON.stringify(fixed.id)}))?.getAttribute('transform')`), fixed.transform, "reduced-motion activity stays still");
  await click(button("Pause flow"));
  await click("document.querySelector('button[aria-label=" + JSON.stringify("Reset workflow") + "]')");
  await waitFor("document.querySelectorAll('.kl-flow-token').length===0");
  evidence.reducedMotion = true;
  for (const path of ["/guide", "/practice", "/practice/easy"]) { await navigate(path); assert(await evaluate("!!document.querySelector('.kl-site-footer')")); }
  assert.equal((await fetch(`${origin}/customize`)).status, 404);
  evidence.routeSmoke = ["/guide", "/practice", "/practice/easy", "/customize:404"];
  assert.deepEqual(evidence.errors, []);
  evidence.status = "PASS";
  console.log(JSON.stringify({ status: evidence.status, charts: Object.keys(evidence.charts), mobile390: evidence.mobile390,
    mobile320: evidence.mobile320, offsetOverlap: evidence.offsetOverlap, reducedMotion: evidence.reducedMotion, errors: evidence.errors }, null, 2));
} catch (error) {
  if (captureFailure) await captureFailure().catch(() => {});
  evidence.status = "FAIL";
  evidence.failure = error.stack;
  console.error(error);
  process.exitCode = 1;
} finally {
  await writeFile(join(artifacts, "evidence.json"), JSON.stringify(evidence, null, 2));
  socket?.close();
  chrome.kill("SIGTERM");
  await new Promise((resolve) => { if (chrome.exitCode !== null) resolve(); else chrome.once("exit", resolve); });
  await rm(profile, { recursive: true, force: true });
}
