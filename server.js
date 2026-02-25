"use strict";

/* ======================================================
   Imports
====================================================== */
const express = require("express");
const crypto = require("crypto");
const { Firestore } = require("@google-cloud/firestore");
const { CloudTasksClient } = require("@google-cloud/tasks");

const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

/* ======================================================
   App Init
====================================================== */
const app = express();
app.use(express.json({ limit: "1mb" }));

/* ======================================================
   ENV CONFIG
====================================================== */
const ENV = {
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN || "",
  TELEGRAM_CHANNEL_ID: process.env.TELEGRAM_CHANNEL_ID || "",
  WEBENGAGE_LICENSE_CODE: process.env.WEBENGAGE_LICENSE_CODE || "",
  WEBENGAGE_API_KEY: process.env.WEBENGAGE_API_KEY || "",
  GCP_PROJECT: process.env.GCP_PROJECT || "",
  GCP_LOCATION: process.env.GCP_LOCATION || "asia-south1",
  TASKS_QUEUE:
    process.env.TASKS_QUEUE || "final-larger-invite-queue-handler",
  BASE_URL: (process.env.BASE_URL || "").replace(/\/$/, ""),
  CLOUD_RUN_SERVICE_ACCOUNT:
    process.env.CLOUD_RUN_SERVICE_ACCOUNT || "",
  PORT: process.env.PORT || 8080,
};

console.log("=== STARTUP CONFIG ===");
console.log(JSON.stringify(ENV, null, 2));

/* ======================================================
   Init Services
====================================================== */
const db = new Firestore();
const tasks = new CloudTasksClient();

const COL_REQ = "invite_requests";
const COL_INV = "invite_lookup";

const MAX_ATTEMPTS = 10;

/* ======================================================
   Utils
====================================================== */
function trace(label, data) {
  console.log(`[TRACE ${new Date().toISOString()}] ${label}`, data || "");
}

function errorLog(label, err) {
  console.error(`[ERROR ${new Date().toISOString()}] ${label}`, err);
}

function sha256(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}

function now() {
  return new Date().toISOString();
}

/* ======================================================
   Health Endpoint
====================================================== */
app.get("/", (req, res) => {
  res.status(200).send("Service Healthy");
});

/* ======================================================
   WebEngage
====================================================== */
async function fireWebEngage(userId, eventName, eventData) {
  try {
    const url = `https://api.webengage.com/v1/accounts/${ENV.WEBENGAGE_LICENSE_CODE}/events`;

    trace("WEBENGAGE → Sending", { userId, eventName });

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${ENV.WEBENGAGE_API_KEY}`,
      },
      body: JSON.stringify({
        userId: String(userId),
        eventName,
        eventData,
      }),
    });

    const text = await response.text();

    trace("WEBENGAGE → Response", {
      status: response.status,
      ok: response.ok,
      body: text,
    });

    return response.ok;
  } catch (err) {
    errorLog("WEBENGAGE FAILURE", err);
    return false;
  }
}

/* ======================================================
   Cloud Tasks Enqueue
====================================================== */
async function enqueueWorker(requestId, delaySec = 0) {
  try {
    if (!ENV.BASE_URL || !ENV.CLOUD_RUN_SERVICE_ACCOUNT) {
      errorLog("TASK ENQUEUE FAILED", "Missing BASE_URL or SA");
      return;
    }

    const parent = tasks.queuePath(
      ENV.GCP_PROJECT,
      ENV.GCP_LOCATION,
      ENV.TASKS_QUEUE
    );

    const task = {
      httpRequest: {
        httpMethod: "POST",
        url: `${ENV.BASE_URL}/v1/invite/worker`,
        headers: { "Content-Type": "application/json" },
        body: Buffer.from(JSON.stringify({ requestId })).toString(
          "base64"
        ),
        oidcToken: {
          serviceAccountEmail: ENV.CLOUD_RUN_SERVICE_ACCOUNT,
        },
      },
    };

    if (delaySec > 0) {
      task.scheduleTime = {
        seconds: Math.floor(Date.now() / 1000) + delaySec,
      };
    }

    await tasks.createTask({ parent, task });

    trace("TASK ENQUEUED", { requestId, delaySec });
  } catch (err) {
    errorLog("TASK CREATE ERROR", err);
  }
}

/* ======================================================
   Telegram Invite Creation
====================================================== */
async function createTelegramLink(name) {
  try {
    trace("TELEGRAM → Creating Invite", name);

    const res = await fetch(
      `https://api.telegram.org/bot${ENV.TELEGRAM_BOT_TOKEN}/createChatInviteLink`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: ENV.TELEGRAM_CHANNEL_ID,
          member_limit: 1,
          name,
        }),
      }
    );

    const json = await res.json();

    trace("TELEGRAM → Response", json);

    return { ok: res.ok, json };
  } catch (err) {
    errorLog("TELEGRAM NETWORK ERROR", err);
    return { ok: false, json: null };
  }
}

/* ======================================================
   1) Create Invite Request
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
      error: null,
    });

    trace("INVITE REQUEST CREATED", requestId);

    await enqueueWorker(requestId);

    res.json({ ok: true, requestId, status: "QUEUED" });
  } catch (err) {
    errorLog("REQUEST ERROR", err);
    res.status(500).json({ error: "Internal error" });
  }
});

/* ======================================================
   2) Worker
====================================================== */
app.post("/v1/invite/worker", async (req, res) => {
  try {
    const { requestId } = req.body;
    if (!requestId) return res.status(400).send("Missing requestId");

    trace("WORKER STARTED", requestId);

    const ref = db.collection(COL_REQ).doc(requestId);
    const snap = await ref.get();
    if (!snap.exists) return res.send("No doc");

    const doc = snap.data();

    if (doc.status === "DONE" || doc.status === "FAILED")
      return res.send("Already processed");

    const attempts = doc.attempts + 1;

    if (attempts > MAX_ATTEMPTS) {
      await ref.update({ status: "FAILED", updatedAt: now() });
      return res.send("Max attempts reached");
    }

    await ref.update({ status: "PROCESSING", attempts });

    const name = `uid:${doc.userId}|rid:${requestId}|a:${attempts}`.slice(
      0,
      255
    );

    const tg = await createTelegramLink(name);

    if (!tg.ok) {
      if (tg.json?.error_code === 429) {
        const retryAfter =
          Number(tg.json?.parameters?.retry_after) || 10;

        await ref.update({ status: "QUEUED" });
        await enqueueWorker(requestId, retryAfter);
      } else {
        await ref.update({
          status: "FAILED",
          error: tg.json?.description || "telegram_error",
        });
      }

      return res.send("Telegram handled");
    }

    const inviteLink = tg.json.result.invite_link;
    const inviteHash = sha256(inviteLink);

    await db.collection(COL_INV).doc(inviteHash).set({
      inviteLink,
      requestId,
      userId: doc.userId,
      createdAt: now(),
    });

    await ref.update({
      status: "DONE",
      inviteLink,
      updatedAt: now(),
    });

    trace("INVITE CREATED SUCCESS", inviteLink);

    const weOk = await fireWebEngage(
      doc.userId,
      "pass_paid_community_telegram_link_created",
      { inviteLink }
    );

    await ref.update({ weLinkEventFired: weOk });

    trace("WEBENGAGE EVENT RESULT", weOk);

    res.send("Worker success");
  } catch (err) {
    errorLog("WORKER CRASH", err);
    res.status(500).send("Worker failed");
  }
});

/* ======================================================
   Status Endpoint
====================================================== */
app.get("/v1/invite/status/:requestId", async (req, res) => {
  try {
    const snap = await db
      .collection(COL_REQ)
      .doc(req.params.requestId)
      .get();

    if (!snap.exists)
      return res.status(404).json({ error: "Not found" });

    res.json(snap.data());
  } catch (err) {
    errorLog("STATUS ERROR", err);
    res.status(500).json({ error: "Status failed" });
  }
});

/* ======================================================
   Start Server
====================================================== */
app.listen(ENV.PORT, () => {
  trace("SERVER STARTED", `Listening on port ${ENV.PORT}`);
});
