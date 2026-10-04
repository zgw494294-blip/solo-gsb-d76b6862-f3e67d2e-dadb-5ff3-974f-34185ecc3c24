"use strict";

let expId = null;
const lastRender = { a: "", b: "" };

const $ = (id) => document.getElementById(id);

class ExpGone extends Error {}

async function fetchJson(path, options) {
  const resp = await fetch(path, options);
  if (resp.status === 404) {
    // 服务端重启后旧实验已不存在：自动新建一个，再通知调用方放弃本次请求
    await newExperiment();
    throw new ExpGone();
  }
  if (!resp.ok) {
    throw new Error("HTTP " + resp.status);
  }
  return resp.json();
}

async function newExperiment() {
  const resp = await fetch("/api/experiments", { method: "POST" });
  const data = await resp.json();
  expId = data.id;
  history.replaceState(null, "", "?exp=" + expId);
  $("exp-id").textContent = expId;
  clearError("a");
  clearError("b");
  await refreshStatus();
  await refreshCommitted();
}

async function refreshStatus() {
  if (!expId) return;
  const data = await fetchJson(`/api/experiments/${expId}/status`);
  renderPanel("a", data.a);
  renderPanel("b", data.b);
}

async function refreshCommitted() {
  if (!expId) return;
  try {
    const data = await fetchJson(`/api/experiments/${expId}/committed`);
    $("committed").textContent = data.committed;
  } catch (e) {
    if (!(e instanceof ExpGone)) {
      // 忽略瞬时错误，下一轮轮询会恢复
    }
  }
}

function renderPanel(side, state) {
  const key = JSON.stringify(state);
  if (lastRender[side] === key) return;
  lastRender[side] = key;

  const inTx = state.in_transaction;
  $("tx-" + side).textContent = inTx ? "已开启" : "未开启";
  const dot = $("dot-" + side);
  dot.classList.toggle("on", inTx);
  dot.title = inTx ? "事务已开启" : "事务未开启";
  $("read-" + side).textContent = state.last_read === null ? "—" : state.last_read;

  const logEl = $("log-" + side);
  logEl.innerHTML = "";
  for (const entry of state.log) {
    const li = document.createElement("li");
    li.textContent = entry;
    if (entry.includes("→ 失败") || entry.includes("→ 拒绝")) {
      li.classList.add("log-error");
    }
    logEl.appendChild(li);
  }
  logEl.scrollTop = logEl.scrollHeight;
}

function setBusy(side, busy) {
  // 只禁用本栏的按钮与输入框，另一栏不受影响
  for (const el of document.querySelectorAll(
    `#panel-${side} button, #panel-${side} input`
  )) {
    el.disabled = busy;
  }
}

function showError(side, error) {
  const box = $("error-" + side);
  box.textContent = error.name
    ? `${error.name}（错误码 ${error.code}）：${error.message}`
    : error.message;
  box.hidden = false;
}

function clearError(side) {
  $("error-" + side).hidden = true;
}

async function doOp(side, op) {
  const body = { op };
  if (op === "add") {
    const raw = $(`add-input-${side}`).value.trim();
    if (!/^[+-]?\d+$/.test(raw)) {
      showError(side, { name: null, code: null, message: "请输入一个整数" });
      return;
    }
    // 大整数以十进制字符串传输避免精度丢失；安全整数范围内仍发送数字
    const big = BigInt(raw);
    body.value =
      big >= BigInt(Number.MIN_SAFE_INTEGER) &&
      big <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(big)
        : big.toString();
  }
  setBusy(side, true);
  clearError(side);
  try {
    const data = await fetchJson(`/api/experiments/${expId}/conn/${side}/op`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    renderPanel(side, data.state);
    if (!data.ok && data.error) {
      showError(side, data.error);
    }
    refreshCommitted();
  } catch (e) {
    if (!(e instanceof ExpGone)) {
      showError(side, { name: null, code: null, message: "请求失败：" + e.message });
    }
  } finally {
    setBusy(side, false);
  }
}

function bindPanel(side) {
  for (const btn of document.querySelectorAll(`#panel-${side} button[data-op]`)) {
    btn.addEventListener("click", () => doOp(side, btn.dataset.op));
  }
}

async function init() {
  bindPanel("a");
  bindPanel("b");
  $("new-exp").addEventListener("click", () => {
    newExperiment().catch(() => {});
  });
  $("refresh-committed").addEventListener("click", () => {
    refreshCommitted();
  });

  const params = new URLSearchParams(location.search);
  const existing = params.get("exp");
  if (existing) {
    expId = existing;
    $("exp-id").textContent = existing;
    try {
      await refreshStatus();
      await refreshCommitted();
    } catch (e) {
      // 404 时 fetchJson 已自动新建实验
    }
  } else {
    await newExperiment();
  }

  // 状态轮询只读取服务端内存状态，不执行任何 SELECT，不会提前建立快照
  setInterval(() => {
    refreshStatus().catch(() => {});
  }, 2000);
  setInterval(() => {
    refreshCommitted();
  }, 2000);
}

init();
