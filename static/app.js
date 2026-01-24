


function track(name, data) {
  try {
    if (window.umami && typeof window.umami.track === "function") {
      window.umami.track(name, data);
    }
  } catch {}
}
const $id = (id) => document.getElementById(id);



const schemaForm = $id("schemaForm");
const schemaFile = $id("schemaFile");
const schemaId = $id("schemaId");
const question = $id("question");
const generateBtn = $id("generateBtn");
const resetBtn = $id("resetBtn");

schemaId.addEventListener("input", syncGenerateEnabled);


const banner = $id("banner");
const sqlOut = $id("sqlOut");
const valOut = $id("valOut");
const msgOut = $id("msgOut");
const explainOut = $id("explainOut");
const examplesEl = $id("examples");

const copySqlBtn = $id("copySqlBtn");
const copyValBtn = $id("copyValBtn");

const charCount = $id("charCount");
const schemasRemaining = $id("schemasRemaining");
const generatesRemaining = $id("generatesRemaining");
const schemaInfo = $id("schemaInfo");


function showBanner(text) {
  banner.textContent = text;
  banner.classList.remove("hidden");
}
function hideBanner() {
  banner.textContent = "";
  banner.classList.add("hidden");
}

function setLoading(isLoading) {
  generateBtn.disabled = isLoading;
  schemaFile.disabled = isLoading;
  question.disabled = isLoading;
  schemaId.disabled = isLoading;
  generateBtn.textContent = isLoading ? "Generating..." : "Generate SQL";
}
function syncGenerateEnabled() {
  const hasSchema = (schemaId.value || "").trim().length > 0;
  generateBtn.disabled = !hasSchema;
  generateBtn.title = hasSchema ? "" : "Upload schema.sql first";
}


function setUsageFromHeaders(res) {
  const remG = res.headers.get("X-RateLimit-Remaining-Generates");
  const remS = res.headers.get("X-RateLimit-Remaining-Schemas");
  if (remG !== null) generatesRemaining.textContent = remG;
  if (remS !== null) schemasRemaining.textContent = remS;
}

function saveSchemaId(id) {
  localStorage.setItem("qw_schema_id", id);
  schemaId.value = id;
  syncGenerateEnabled();
}


function loadSchemaId() {
  const id = localStorage.getItem("qw_schema_id");
  if (id) schemaId.value = id;
  syncGenerateEnabled();
}


question.addEventListener("input", () => {
  charCount.textContent = String(question.value.length);
});

schemaForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  track("schema_upload_attempted");

  hideBanner();
  setLoading(true);

  try {
    const file = schemaFile.files[0];
    if (!file) throw new Error("Select a schema.sql file first.");
        // Front-end gate: don't even send non-.sql files
        const name = (file.name || "").toLowerCase();
        if (!name.endsWith(".sql")) {
          throw new Error("Invalid file type. Please upload a .sql schema file.");
        }
    

    const form = new FormData();
    form.append("file", file, file.name);

    const res = await fetch("/schema", { method: "POST", body: form });
        // HARD stop on non-2xx (FastAPI HTTPException returns {detail: "..."} with no schema_id)
        if (!res.ok) {
          const errData = await res.json().catch(() => ({}));
          showBanner(errData.detail || "Schema upload failed.");
          setLoading(false);
          return;
        }
    
    setUsageFromHeaders(res);

    const data = await res.json();

    if (data.status && data.status === "error") {
      showBanner(data.message || "Schema upload failed.");
      setLoading(false);
      return;
    }

    const sid = data.schema_id;
    if (!sid) {
      showBanner("Schema upload failed: server did not return schema_id.");
      setLoading(false);
      return;
    }
    saveSchemaId(sid);

    schemaInfo.textContent =
      `Uploaded. Tables: ${data.summary?.tables ?? "?"}, Columns: ${data.summary?.columns ?? "?"}. Preview: ${JSON.stringify(data.schema_preview ?? [])}`;

  } catch (err) {
    showBanner(err.message || String(err));
  } finally {
    setLoading(false);
  }
});

generateBtn.addEventListener("click", async (e) => {
  e.preventDefault();
  track("generate_clicked");

  hideBanner();
  setLoading(true);
  sqlOut.textContent = "";
  valOut.textContent = "";
  msgOut.textContent = "";
  explainOut.textContent = "";


  try {
    const sid = schemaId.value.trim();
    const q = question.value.trim();
    if (!sid) throw new Error("Upload a schema first (schema_id is required).");
    if (!q) throw new Error("Type a question first.");

    const res = await fetch("/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ schema_id: sid, question: q }),
    });

    setUsageFromHeaders(res);
    const data = await res.json();

    if (data.status === "error") {
      showBanner(data.message || "Request failed.");
      msgOut.textContent = JSON.stringify(data, null, 2);
      return;
    }

    sqlOut.textContent = data.sql || "";
    valOut.textContent = JSON.stringify(data.validation || {}, null, 2);
    msgOut.textContent = data.message || JSON.stringify(data.classification || {}, null, 2);
    explainOut.textContent = buildExplanation(data);


  } catch (err) {
    showBanner(err.message || String(err));
  } finally {
    setLoading(false);
  }
});

resetBtn.addEventListener("click", () => {
  localStorage.removeItem("qw_schema_id");  // or whatever key you used
  schemaId.value = "";
  syncGenerateEnabled();

  showBanner("Schema reset.");
  setTimeout(hideBanner, 900);
});


if (copySqlBtn) {
  copySqlBtn.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(sqlOut.textContent || "");
      showBanner("Copied SQL to clipboard.");
      setTimeout(hideBanner, 900);
      track("copy_sql_clicked");
    } catch {
      showBanner("Copy failed (browser permissions).");
    }
  });
}

if (copyValBtn) {
  copyValBtn.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(valOut.textContent || "");
      showBanner("Copied validation JSON to clipboard.");
      setTimeout(hideBanner, 900);
      track("copy_json_clicked");
    } catch {
      showBanner("Copy failed (browser permissions).");
    }
  });
}





const EXAMPLES = [
  "List all employees with their branch name",
  "Show total employees per branch",
  "List branches with zero employees",
  "List the first 5 employees with their branch name and branch location"
];

function renderExamples() {
  if (!examplesEl) return;

  examplesEl.innerHTML = EXAMPLES.map((t) =>
    `<button type="button" class="exbtn" data-example="${encodeURIComponent(t)}">${t}</button>`
  ).join("");

  examplesEl.querySelectorAll("button[data-example]").forEach((btn) => {
    btn.addEventListener("click", () => {
      track("example_clicked");
      const text = decodeURIComponent(btn.getAttribute("data-example") || "");
      question.value = text;
      charCount.textContent = String(question.value.length);
      question.focus();
    });
  });
}
function buildExplanation(data) {
  const v = data.validation || {};
  const tables = Array.isArray(v.tables_detected) ? v.tables_detected : [];

  const parts = [];

  if (tables.length) parts.push(`Tables used: ${tables.join(", ")}`);
  else parts.push("Tables used: (none detected)");

  if (v.alias_map && typeof v.alias_map === "object" && Object.keys(v.alias_map).length) {
    parts.push(`Aliases: ${JSON.stringify(v.alias_map)}`);
  }

  if (Array.isArray(v.join_warnings) && v.join_warnings.length) {
    parts.push(`Join notes: ${v.join_warnings.join("; ")}`);
  } else {
    parts.push("Join notes: none");
  }

  if (data.message) parts.push(`Note: ${data.message}`);

  return parts.join("\n");
}


// init
loadSchemaId();
charCount.textContent = String(question.value.length);
renderExamples();

