"use strict";

const express = require("express");
const crypto = require("crypto");
const { Firestore, FieldValue } = require("@google-cloud/firestore");
const { CloudTasksClient } = require("@google-cloud/tasks");

const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

const app = express();
app.use(express.json({ limit: "1mb" }));

/* =========================
   ENV
========================= */
const {
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHANNEL_ID,
  WEBENGAGE_LICENSE_CODE,
  WEBENGAGE_API_KEY,
  GCP_PROJECT,
  GCP_LOCATION = "asia-south1",
  TASKS_QUEUE = "tg-invite-queue",
  BASE_URL,
  PORT = 8080,
} = process.env;

const db = new Firestore();
const tasks = new CloudTasksClient();

const COL_REQ = "invite_requests";
const COL_INV = "invite_lookup";

const MAX_ATTEMPTS = 50;

/* =========================
   Utils
========================= */
const trace = (tag, msg, data = null) => {
  console.log(
    `[${tag}] ${msg}${data ? " | DATA: " + JSON.stringify(data) : ""}`
  );
};

const sha256 = (s) =>
  crypto.createHash("sha256").update(String(s)).digest("hex");

const now = () => new Date().toISOString();

/* =========================
   WebEngage
========================= */
async function fireWebEngage(userId, eventName, eventData) {
  const url = `https://api.webengage.com/v1/accounts/${WEBENGAGE_LICENSE_CODE}/events`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${WEBENGAGE_API_KEY}`,
    },
    body: JSON.stringify({
      userId: String(userId),
      eventName,
      eventData,
    }),
  });

  return res.ok;
}

/* =========================
   Cloud Tasks Enqueue
========================= */
async function enqueueWorker(requestId, delaySec = 0) {
  const parent = tasks.queuePath(GCP_PROJECT, GCP_LOCATION, TASKS_QUEUE);

  const task = {
    httpRequest: {
      httpMethod: "POST",
      url: `${BASE_URL}/v1/invite/worker`,
      headers: { "Content-Type": "application/json" },
      body: Buffer.from(JSON.stringify({ requestId })).toString("base64"),
    },
  };

  if (delaySec > 0) {
    task.scheduleTime = {
      seconds: Math.floor(Date.now() / 1000) + delaySec,
    };
  }

  await tasks.createTask({ parent, task });
}

/* =========================
   Telegram
========================= */
async function createTelegramLink(name) {
  const res = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/createChatInviteLink`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHANNEL_ID,
        member_limit: 1,
        name,
      }),
    }
  );

  const json = await res.json();
  return { ok: res.ok, json };
}

/* ======================================================
   1) INVITE REQUEST (FAST RETURN)
====================================================== */
app.post("/v1/invite/request", async (req, res) => {
  const { userId, transactionId = "" } = req.body;

  if (!userId) return res.status(400).json({ error: "Missing userId" });

  const requestId = crypto.randomUUID();

  await db.collection(COL_REQ).doc(requestId).set({
    requestId,
    userId,
    transactionId,
    status: "QUEUED",
    attempts: 0,
    createdAt: now(),
    updatedAt: now(),
    weLinkEventFired: false,
    joinEventFired: false,
  });

  await enqueueWorker(requestId);

  res.json({ ok: true, status: "queued", requestId });
});

/* ======================================================
   2) WORKER (CALLED BY CLOUD TASKS)
====================================================== */
app.post("/v1/invite/worker", async (req, res) => {
  if (!req.headers["x-cloudtasks-queuename"]) {
    return res.status(403).send("Forbidden");
  }

  const { requestId } = req.body;
  if (!requestId) return res.status(400).send("Missing requestId");

  const ref = db.collection(COL_REQ).doc(requestId);
  const snap = await ref.get();
  if (!snap.exists) return res.send("ok");

  const doc = snap.data();
  if (doc.status === "DONE") return res.send("ok");

  const attempts = doc.attempts + 1;
  if (attempts > MAX_ATTEMPTS) {
    await ref.update({ status: "FAILED", updatedAt: now() });
    return res.send("ok");
  }

  await ref.update({
    status: "PROCESSING",
    attempts,
    updatedAt: now(),
  });

  const name = `uid:${doc.userId}|txn:${doc.transactionId}|rid:${requestId}|a:${attempts}`.slice(
    0,
    255
  );

  const tg = await createTelegramLink(name);

  if (!tg.ok) {
    const retryAfter =
      Number(tg.json?.parameters?.retry_after) || 10;

    await ref.update({ status: "QUEUED" });

    await enqueueWorker(requestId, retryAfter);

    return res.send("retry scheduled");
  }

  const inviteLink = tg.json?.result?.invite_link;
  const inviteHash = sha256(inviteLink);

  await db.collection(COL_INV).doc(inviteHash).set({
    inviteLink,
    requestId,
    userId: doc.userId,
    transactionId: doc.transactionId,
    createdAt: now(),
  });

  await ref.update({
    status: "DONE",
    inviteLink,
    updatedAt: now(),
  });

  if (!doc.weLinkEventFired) {
    const ok = await fireWebEngage(
      doc.userId,
      "pass_paid_community_telegram_link_created",
      {
        transactionId: doc.transactionId,
        inviteLink,
      }
    );

    await ref.update({ weLinkEventFired: ok });
  }

  res.send("ok");
});

/* ======================================================
   3) TELEGRAM WEBHOOK
====================================================== */
app.post("/v1/telegram/webhook", async (req, res) => {
  const upd = req.body.chat_member || req.body.my_chat_member;
  if (!upd) return res.send("ignored");

  const inviteLink = upd?.invite_link?.invite_link;
  const status = upd?.new_chat_member?.status;
  const telegramUserId = upd?.new_chat_member?.user?.id;

  if (
    !inviteLink ||
    !telegramUserId ||
    !["member", "administrator", "creator"].includes(status)
  ) {
    return res.send("ignored");
  }

  const inviteHash = sha256(inviteLink);
  const linkSnap = await db.collection(COL_INV).doc(inviteHash).get();
  if (!linkSnap.exists) return res.send("orphan");

  const { requestId } = linkSnap.data();

  const reqRef = db.collection(COL_REQ).doc(requestId);
  const reqSnap = await reqRef.get();
  if (!reqSnap.exists) return res.send("ok");

  const reqDoc = reqSnap.data();
  if (reqDoc.joinEventFired) return res.send("ok");

  const ok = await fireWebEngage(
    reqDoc.userId,
    "pass_paid_community_telegram_joined",
    {
      transactionId: reqDoc.transactionId,
      inviteLink,
      telegramUserId: String(telegramUserId),
    }
  );

  await reqRef.update({
    joinEventFired: ok,
    joinedAt: now(),
    telegramUserId,
    updatedAt: now(),
  });

  res.send("ok");
});

/* ========================= */
app.listen(PORT, () =>
  trace("SYSTEM", `Listening on ${PORT}`)
);
