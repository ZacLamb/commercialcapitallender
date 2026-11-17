// Lendtrack -> GHL webhook receiver
// 1) Capture mode (default if GHL_TOKEN unset): logs every payload so we can see Lendtrack's real shape.
// 2) Sync mode: upserts the GHL contact and, if a stage/status is present and mapped, moves the opportunity.
const express = require("express");

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

const {
  PORT = 3000,
  WEBHOOK_SECRET, // goes in the URL path: /webhook/<secret>
  ADMIN_KEY, // for GET /admin/recent?key=...
  GHL_TOKEN, // Private Integration Token (contacts + opportunities scopes)
  GHL_LOCATION_ID,
  GHL_PIPELINE_ID, // optional: restrict stage moves to this pipeline
  STAGE_MAP = "{}", // JSON: {"collecting docs":"<stageId>","funding review":"<stageId>","funded":"<stageId>"}
  EXTRA_TAGS = "lendtrack", // comma separated
} = process.env;

const recent = []; // last 100 events, in memory
const GHL = "https://services.leadconnectorhq.com";
const stageMap = Object.fromEntries(
  Object.entries(JSON.parse(STAGE_MAP)).map(([k, v]) => [k.trim().toLowerCase(), v])
);

// Lendtrack's payload shape is unknown, so look in several likely places/names.
function pick(obj, keys) {
  const flat = {};
  (function walk(o, prefix) {
    if (o && typeof o === "object") {
      for (const [k, v] of Object.entries(o)) {
        if (v && typeof v === "object" && !Array.isArray(v)) walk(v, prefix + k + ".");
        else flat[(prefix + k).toLowerCase().replace(/[\s_-]/g, "")] = v;
      }
    }
  })(obj, "");
  for (const key of keys) {
    for (const [fk, v] of Object.entries(flat)) {
      if ((fk === key || fk.endsWith("." + key)) && v != null && v !== "") return String(v);
    }
  }
  return undefined;
}

function normalize(p) {
  const full = pick(p, ["name", "fullname", "contactname", "ownername"]);
  let first = pick(p, ["firstname", "first"]);
  let last = pick(p, ["lastname", "last"]);
  if (!first && full) [first, ...[last]] = [full.split(" ")[0], full.split(" ").slice(1).join(" ") || last];
  return {
    id: pick(p, ["leadid", "id", "applicationid", "dealid"]),
    email: pick(p, ["email", "emailaddress", "owneremail"]),
    phone: pick(p, ["phone", "phonenumber", "mobile", "ownerphone"]),
    firstName: first,
    lastName: last,
    companyName: pick(p, ["businessname", "companyname", "company", "legalname"]),
    status: pick(p, ["status", "stage", "pipelinestage", "dealstatus"]),
    partner: pick(p, ["partner", "partnername", "referralpartner", "source"]),
    product: pick(p, ["product", "producttype", "loantype", "fundingtype"]),
    amount: pick(p, ["amount", "requestedamount", "fundingamount", "loanamount"]),
  };
}

async function ghl(path, method = "GET", body) {
  const r = await fetch(GHL + path, {
    method,
    headers: {
      Authorization: `Bearer ${GHL_TOKEN}`,
      Version: "2021-07-28",
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (!r.ok) throw new Error(`GHL ${method} ${path} -> ${r.status} ${text.slice(0, 300)}`);
  return json;
}

async function sync(n) {
  if (!n.email && !n.phone) return { skipped: "no email or phone" };
  const tags = EXTRA_TAGS.split(",").map((t) => t.trim()).filter(Boolean);
  if (n.partner) tags.push(`partner: ${n.partner}`);
  if (n.status) tags.push(`lt-stage: ${n.status}`);
  const up = await ghl("/contacts/upsert", "POST", {
    locationId: GHL_LOCATION_ID,
    email: n.email,
    phone: n.phone,
    firstName: n.firstName,
    lastName: n.lastName,
    companyName: n.companyName,
    source: "Lendtrack",
    tags,
  });
  const contactId = up.contact && up.contact.id;
  const result = { contactId, new: up.new };

  const stageId = n.status && stageMap[n.status.trim().toLowerCase()];
  if (contactId && stageId) {
    const q = new URLSearchParams({ location_id: GHL_LOCATION_ID, contact_id: contactId });
    if (GHL_PIPELINE_ID) q.set("pipeline_id", GHL_PIPELINE_ID);
    const found = await ghl(`/opportunities/search?${q}`);
    const opp = (found.opportunities || [])[0];
    if (opp) {
      await ghl(`/opportunities/${opp.id}`, "PUT", {
        pipelineStageId: stageId,
        ...(n.amount && !isNaN(Number(n.amount)) ? { monetaryValue: Number(n.amount) } : {}),
      });
      result.movedOpportunity = opp.id;
    } else {
      result.note = "no opportunity found for contact (native sync may not have created one yet)";
    }
  }
  return result;
}

app.post("/webhook/:secret", async (req, res) => {
  if (!WEBHOOK_SECRET || req.params.secret !== WEBHOOK_SECRET) return res.sendStatus(404);
  const event = { at: new Date().toISOString(), payload: req.body, normalized: normalize(req.body) };
  console.log("LENDTRACK EVENT", JSON.stringify(event));
  res.json({ ok: true }); // ack fast so Lendtrack/Zapier doesn't retry
  if (GHL_TOKEN && GHL_LOCATION_ID) {
    try { event.result = await sync(event.normalized); }
    catch (e) { event.error = e.message; console.error(e.message); }
  } else {
    event.result = "capture mode (GHL_TOKEN/GHL_LOCATION_ID not set)";
  }
  recent.unshift(event);
  recent.length = Math.min(recent.length, 100);
});

app.get("/admin/recent", (req, res) => {
  if (!ADMIN_KEY || req.query.key !== ADMIN_KEY) return res.sendStatus(404);
  res.json(recent);
});

app.get("/health", (_req, res) => res.json({ ok: true, mode: GHL_TOKEN ? "sync" : "capture" }));

app.listen(PORT, () => console.log(`lendtrack receiver on ${PORT} (${GHL_TOKEN ? "sync" : "capture"} mode)`));
