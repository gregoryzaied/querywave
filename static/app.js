function $(id) { return document.getElementById(id); }

const schemaForm = $("schemaForm");
const schemaFile = $("schemaFile");
const schemaId = $("schemaId");
const question = $("question");
const generateBtn = $("generateBtn");
const resetBtn = $("resetBtn");

const banner = $("banner");
const sqlOut = $("sqlOut");
const valOut = $("valOut");
const msgOut = $("msgOut");

const copySqlBtn = $("copySqlBtn");
const copyValBtn = $("copyValBtn");

const charCount = $("charCount");
const schemasRemaining = $("schemasRemaining");
const generatesRemaining = $("generatesRemaining");
const schemaInfo = $("schemaInfo");

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

function setUsageFromHeaders(res) {
  const remG = res.headers.get("X-RateLimit-Remaining-Generates");
  const remS = res.headers.get("X-RateLimit-Remaining-Schemas");
  if (remG !== null) generatesRemaining.textContent = remG;
  if (remS !== null) schemasRemaining.textContent = remS;
}

function saveSchemaId(id) {
  localStorage.setItem("qw_schema_id", id);
  schemaId.value = id;
}

function loadSchemaId() {
  const id = localStorage.getItem("qw_schema_id");
  if (id) schemaId.value = id;
}

question.addEventListener("input", () => {
  charCount.textContent = String(question.value.length);
});

schemaForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  hideBanner();
  setLoading(true);

  try {
    const file = schemaFile.files[0];
    if (!file) throw new Error("Select a schema.sql file first.");

    const form = new FormData();
    form.append("file", file, file.name);

    const res = await fetch("/schema", { method: "POST", body: form });
    setUsageFromHeaders(res);

    const data = await res.json();

    if (data.status && data.status === "error") {
      showBanner(data.message || "Schema upload failed.");
      setLoading(false);
      return;
    }

    // If your /schema returns envelope: status ok + schema_id etc.
    const sid = data.schema_id || (data.status === "ok" ? data.schema_id : null);
    if (!sid && data.schema_id === undefined) {
      // handle plain schema endpoint response
      if (data.schema_id) {}
    }

    saveSchemaId(data.schema_id);
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
  hideBanner();
  setLoading(true);
  sqlOut.textContent = "";
  valOut.textContent = "";
  msgOut.textContent = "";

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

  } catch (err) {
    showBanner(err.message || String(err));
  } finally {
    setLoading(false);
  }
});

resetBtn.addEventListener("click", () => {
  localStorage.removeItem("qw_schema_id");
  schemaId.value = "";
  schemaInfo.textContent = "";
  sqlOut.textContent = "";
  valOut.textContent = "";
  msgOut.textContent = "";
  hideBanner();
});

copySqlBtn.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(sqlOut.textContent || "");
    showBanner("Copied SQL to clipboard.");
    setTimeout(hideBanner, 900);
  } catch {
    showBanner("Copy failed (browser permissions).");
  }
});

copyValBtn.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(valOut.textContent || "");
    showBanner("Copied validation JSON to clipboard.");
    setTimeout(hideBanner, 900);
  } catch {
    showBanner("Copy failed (browser permissions).");
  }
});

// init
loadSchemaId();
charCount.textContent = String(question.value.length);
