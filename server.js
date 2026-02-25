"use strict";

const express = require("express");
const crypto = require("crypto");
const { Firestore } = require("@google-cloud/firestore");
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
  TASKS_QUEUE = "final-larger-invite-queue-handler",
  BASE_URL,
  CLOUD_RUN_SERVICE_ACCOUNT, // REQUIRED for OIDC auth
  PORT = 8080,
} = process.env;

if (!BASE_URL) throw new Error("BASE_URL missing");
if (!CLOUD_RUN_SERVICE_ACCOUNT)
  throw new Error("CLOUD_RUN_SERVICE_ACCOUNT missing");

/* =========================
   Init
========================= */
const db = new Firestore();
const tasks = new CloudTasksClient();

const COL_REQ = "invite_requests";
const COL_INV = "invite_lookup";

const MAX_ATTEMPTS = 10; // reduced from 50

/* =========================
   Utils
========================= */
const trace = (tag, msg, data) => {
  console.log(`[${tag}] ${msg}`, data || "");
};

const sha256 = (s) =>
  crypto.createHash("sha256").update(String(s)).digest("hex");

const now = () => new Date().toISOString();

const cleanBaseUrl = BASE_URL.replace(/\/$/, "");

/* =========================
   WebEngage
========================= */
async function fireWebEngage(userId, eventName, eventData) {
  try {
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
  } catch (err) {
    console.error("WEBENGAGE ERROR:", err);
    return false;
  }
}

/* =========================
   Cloud Tasks
========================= */
async function enqueueWorker(requestId, delaySec = 0) {
  const parent = tasks.queuePath(GCP_PROJECT, GCP_LOCATION, TASKS_QUEUE);

  const task = {
    httpRequest: {
      httpMethod: "POST",
      url: `${cleanBaseUrl}/v1/invite/worker`,
      headers: { "Content-Type": "application/json" },
      body: Buffer.from(JSON.stringify({ requestId })).toString("base64"),
      oidcToken: {
        serviceAccountEmail: CLOUD_RUN_SERVICE_ACCOUNT,
      },
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
  try {
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
  } catch (err) {
    console.error("TELEGRAM NETWORK ERROR:", err);
    return { ok: false, json: null };
  }
}

/* ======================================================
   1) CREATE INVITE REQUEST
====================================================== */
app.post("/v1/invite/request", async (req, res) => {
  try {
    const { userId, transactionId = "" } = req.body;
    if (!userId)
      return res.status(400).json({ error: "Missing userId" });

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
  } catch (err) {
    console.error("REQUEST ERROR:", err);
    res.status(500).json({ error: "Server error" });
  }
});

/* ======================================================
   2) WORKER
====================================================== */
app.post("/v1/invite/worker", async (req, res) => {
  try {
    const { requestId } = req.body;
    if (!requestId) return res.status(400).send("Missing requestId");

    const ref = db.collection(COL_REQ).doc(requestId);
    const snap = await ref.get();
    if (!snap.exists) return res.send("ok");

    const doc = snap.data();

    if (doc.status === "DONE" || doc.status === "FAILED")
      return res.send("ok");

    const attempts = doc.attempts + 1;

    if (attempts > MAX_ATTEMPTS) {
      await ref.update({ status: "FAILED", updatedAt: now() });
      return res.send("max attempts reached");
    }

    await ref.update({
      status: "PROCESSING",
      attempts,
      updatedAt: now(),
    });

    const name = `uid:${doc.userId}|txn:${doc.transactionId}|rid:${requestId}|a:${attempts}`.slice(0, 255);

    const tg = await createTelegramLink(name);

    if (!tg.ok) {
      console.error("TELEGRAM ERROR:", tg.json);

      if (tg.json?.error_code === 429) {
        const retryAfter =
          Number(tg.json?.parameters?.retry_after) || 10;

        await ref.update({ status: "QUEUED" });
        await enqueueWorker(requestId, retryAfter);
      } else {
        await ref.update({
          status: "FAILED",
          error: tg.json?.description || "telegram_error",
          updatedAt: now(),
        });
      }

      return res.send("handled telegram failure");
    }

    const inviteLink = tg.json.result.invite_link;
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

    res.send("success");
  } catch (err) {
    console.error("WORKER ERROR:", err);
    res.status(500).send("worker crashed");
  }
});

/* ======================================================
   STATUS CHECK
====================================================== */
app.get("/v1/invite/status/:requestId", async (req, res) => {
  try {
    const snap = await db
      .collection(COL_REQ)
      .doc(req.params.requestId)
      .get();

    if (!snap.exists)
      return res.status(404).json({ error: "Not found" });

    const data = snap.data();

    res.json({
      status: data.status,
      inviteLink: data.inviteLink || null,
      attempts: data.attempts,
      error: data.error || null,
    });
  } catch (err) {
    console.error("STATUS ERROR:", err);
    res.status(500).json({ error: "Server error" });
  }
});

/* ========================= */
app.listen(PORT, () =>
  trace("SYSTEM", `Listening on ${PORT}`)
);
