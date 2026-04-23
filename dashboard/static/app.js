// JobSpy Dashboard — Frontend Logic
const API = "";
const SITES = [
    { id: "indeed", label: "Indeed", default: true },
    { id: "linkedin", label: "LinkedIn", default: true },
    { id: "glassdoor", label: "Glassdoor", default: true },
    { id: "google", label: "Google", default: true },
    { id: "naukri", label: "Naukri", default: true },
    { id: "internshala", label: "Internshala", default: true },
    { id: "foundit", label: "Foundit", default: false },
    { id: "shine", label: "Shine", default: false },
    { id: "timesjobs", label: "TimesJobs", default: false },
    { id: "zip_recruiter", label: "ZipRecruiter", default: false },
    { id: "bayt", label: "Bayt", default: false },
    { id: "bdjobs", label: "BDJobs", default: false },
];

let currentPage = 1;
let pollInterval = null;
const selectedSites = new Set(SITES.filter(s => s.default).map(s => s.id));

// ---- Init ----
document.addEventListener("DOMContentLoaded", () => {
    renderChips();
    bindEvents();
});

function renderChips() {
    const grid = document.getElementById("siteChips");
    grid.innerHTML = SITES.map(s =>
        `<div class="chip ${selectedSites.has(s.id) ? "active" : ""}" data-site="${s.id}">${s.label}</div>`
    ).join("");
    grid.querySelectorAll(".chip").forEach(chip => {
        chip.addEventListener("click", () => {
            const site = chip.dataset.site;
            if (selectedSites.has(site)) { selectedSites.delete(site); chip.classList.remove("active"); }
            else { selectedSites.add(site); chip.classList.add("active"); }
        });
    });
}

function bindEvents() {
    document.getElementById("scrapeBtn").addEventListener("click", startScrape);
    document.getElementById("resetBtn").addEventListener("click", resetScrape);
    document.getElementById("filterText").addEventListener("input", debounce(() => { currentPage = 1; fetchJobs(); }, 400));
    document.getElementById("filterSite").addEventListener("change", () => { currentPage = 1; fetchJobs(); });
    document.getElementById("fresherFilter").addEventListener("change", () => { currentPage = 1; fetchJobs(); });
    document.getElementById("prevBtn").addEventListener("click", () => { if (currentPage > 1) { currentPage--; fetchJobs(); } });
    document.getElementById("nextBtn").addEventListener("click", () => { currentPage++; fetchJobs(); });
    document.getElementById("downloadAllBtn").addEventListener("click", () => downloadCSV(false));
    document.getElementById("downloadFresherBtn").addEventListener("click", () => downloadCSV(true));
    document.getElementById("saveBtn").addEventListener("click", saveToDisk);
}

// ---- Scrape ----
async function startScrape() {
    if (selectedSites.size === 0) { toast("Select at least one site", "error"); return; }

    const config = {
        sites: [...selectedSites],
        search_term: document.getElementById("searchTerm").value,
        location: document.getElementById("location").value,
        country_indeed: document.getElementById("countryIndeed").value,
        results_wanted: document.getElementById("resultsWanted").value,
        hours_old: document.getElementById("hoursOld").value,
        job_type: document.getElementById("jobType").value,
        is_remote: document.getElementById("isRemote").checked,
        description_format: document.getElementById("descFormat").value,
    };

    const btn = document.getElementById("scrapeBtn");
    btn.disabled = true;
    btn.classList.add("loading");
    btn.innerHTML = '<span class="btn-icon">⏳</span> Scraping...';

    try {
        const res = await fetch(`${API}/api/scrape`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(config) });
        if (!res.ok) { const d = await res.json(); toast(d.error || "Failed to start", "error"); resetBtn(); return; }
        toast(`Scraping ${config.sites.length} sites...`, "info");
        setStatus("running", "Scraping...");
        showProgress(true);
        pollInterval = setInterval(pollStatus, 2000);
    } catch (e) { toast("Server error: " + e.message, "error"); resetBtn(); }
}

async function pollStatus() {
    try {
        const res = await fetch(`${API}/api/status`);
        const data = await res.json();
        document.getElementById("progressText").textContent = data.progress;

        if (data.status === "done") {
            clearInterval(pollInterval);
            setStatus("done", `${data.job_count} jobs found`);
            showProgress(false);
            resetBtn();
            toast(`✅ Found ${data.job_count} jobs!`, "success");
            currentPage = 1;
            fetchJobs();
            document.getElementById("filterBar").style.display = "block";
        } else if (data.status === "error") {
            clearInterval(pollInterval);
            setStatus("error", data.error || "Error");
            showProgress(false);
            resetBtn();
            toast("❌ " + (data.error || "Scrape failed"), "error");
        }
    } catch (e) { /* retry silently */ }
}

// ---- Fetch & Render Jobs ----
async function fetchJobs() {
    const filter = document.getElementById("filterText").value;
    const site = document.getElementById("filterSite").value;
    const fresher = document.getElementById("fresherFilter").checked;

    const params = new URLSearchParams({ page: currentPage, per_page: 25, filter, site, fresher });
    try {
        const res = await fetch(`${API}/api/jobs?${params}`);
        const data = await res.json();
        renderTable(data.jobs);
        renderPagination(data);
        updateSiteFilter(data.sites);
        document.getElementById("jobCount").textContent = `${data.total} jobs`;
    } catch (e) { toast("Failed to load jobs", "error"); }
}

function renderTable(jobs) {
    const tbody = document.getElementById("tableBody");
    const table = document.getElementById("dataTable");
    const empty = document.getElementById("emptyState");

    if (!jobs || jobs.length === 0) {
        table.style.display = "none";
        empty.style.display = "flex";
        empty.querySelector("p").innerHTML = "No matching jobs found";
        return;
    }

    table.style.display = "table";
    empty.style.display = "none";

    tbody.innerHTML = jobs.map(j => {
        const site = j.site || "";
        const salary = formatSalary(j);
        const dateStr = j.date_posted ? new Date(j.date_posted).toLocaleDateString("en-IN", { day: "numeric", month: "short" }) : "—";
        const title = escapeHtml(j.title || "—");
        const company = escapeHtml(j.company || "—");
        const location = escapeHtml(j.location || "—");
        const jobType = j.job_type || "—";
        const url = j.job_url || "#";

        return `<tr>
            <td><span class="site-badge ${site}">${site}</span></td>
            <td title="${title}">${title}</td>
            <td title="${company}">${company}</td>
            <td title="${location}">${location}</td>
            <td>${dateStr}</td>
            <td>${jobType}</td>
            <td class="salary-text">${salary}</td>
            <td><a href="${url}" target="_blank" rel="noopener" class="link-btn">View ↗</a></td>
        </tr>`;
    }).join("");
}

function renderPagination(data) {
    const pag = document.getElementById("pagination");
    if (data.total_pages <= 1) { pag.style.display = "none"; return; }
    pag.style.display = "flex";
    document.getElementById("pageInfo").textContent = `Page ${data.page} of ${data.total_pages}`;
    document.getElementById("prevBtn").disabled = data.page <= 1;
    document.getElementById("nextBtn").disabled = data.page >= data.total_pages;
}

function updateSiteFilter(sites) {
    const select = document.getElementById("filterSite");
    const current = select.value;
    const opts = ['<option value="all">All Sites</option>'];
    (sites || []).forEach(s => opts.push(`<option value="${s}">${s}</option>`));
    select.innerHTML = opts.join("");
    if ([...select.options].some(o => o.value === current)) select.value = current;
}

// ---- Downloads ----
function downloadCSV(fresher) {
    const url = `${API}/api/download?fresher=${fresher}`;
    window.open(url, "_blank");
    toast(fresher ? "⬇ Downloading fresher_jobs.csv" : "⬇ Downloading all_jobs.csv", "info");
}

async function saveToDisk() {
    try {
        const res = await fetch(`${API}/api/save`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({}) });
        const data = await res.json();
        if (data.saved) {
            const files = data.files.map(f => `${f.path} (${f.count} jobs)`).join("\n");
            toast(`💾 Saved!\n${data.files.map(f => f.count + " jobs").join(", ")}`, "success");
        } else { toast(data.error || "Save failed", "error"); }
    } catch (e) { toast("Save error: " + e.message, "error"); }
}

// ---- Reset ----
async function resetScrape() {
    clearInterval(pollInterval);
    try { await fetch(`${API}/api/reset`, { method: "POST" }); } catch {}
    setStatus("idle", "Ready");
    resetBtn();
    showProgress(false);
    document.getElementById("filterBar").style.display = "none";
    document.getElementById("dataTable").style.display = "none";
    document.getElementById("emptyState").style.display = "flex";
    document.getElementById("emptyState").querySelector("p").innerHTML = 'Configure your settings and click <strong>Start Scraping</strong>';
    document.getElementById("pagination").style.display = "none";
    document.getElementById("jobCount").textContent = "0 jobs";
    toast("Reset complete", "info");
}

// ---- Helpers ----
function setStatus(status, text) {
    const dot = document.querySelector(".status-dot");
    dot.className = "status-dot " + status;
    document.getElementById("headerStatusText").textContent = text;
}

function showProgress(show) {
    document.getElementById("progressContainer").style.display = show ? "block" : "none";
}

function resetBtn() {
    const btn = document.getElementById("scrapeBtn");
    btn.disabled = false;
    btn.classList.remove("loading");
    btn.innerHTML = '<span class="btn-icon">🚀</span> Start Scraping';
}

function formatSalary(j) {
    if (j.min_amount && j.max_amount) {
        const currency = j.currency || "INR";
        const fmt = (n) => {
            n = parseFloat(n);
            if (currency === "INR" && n >= 100000) return "₹" + (n / 100000).toFixed(1) + "L";
            if (n >= 1000) return (currency === "INR" ? "₹" : "$") + (n / 1000).toFixed(0) + "K";
            return (currency === "INR" ? "₹" : "$") + n;
        };
        return `${fmt(j.min_amount)} - ${fmt(j.max_amount)}`;
    }
    if (j.stipend) return j.stipend;
    return "—";
}

function escapeHtml(str) {
    const d = document.createElement("div");
    d.textContent = str;
    return d.innerHTML;
}

function debounce(fn, ms) {
    let t;
    return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

function toast(msg, type = "info") {
    const c = document.getElementById("toastContainer");
    const t = document.createElement("div");
    t.className = `toast ${type}`;
    t.textContent = msg;
    c.appendChild(t);
    setTimeout(() => { t.style.opacity = "0"; t.style.transform = "translateX(100%)"; setTimeout(() => t.remove(), 300); }, 4000);
}
