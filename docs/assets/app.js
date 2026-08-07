// Fully client-side semantic search.
//
// docs/data/papers.json (built by .github/workflows/build-search-index.yml
// and served as a static file by GitHub Pages) holds arXiv papers pre-embedded
// with sentence-transformers/all-MiniLM-L6-v2. On search, we load the same
// model in-browser as an ONNX build via transformers.js, embed the query
// locally, and rank papers by cosine similarity — no server, no API key,
// no database. The user's query never leaves their device.

import { pipeline, env } from "https://cdn.jsdelivr.net/npm/@xenova/transformers@2.17.2/+esm";

env.allowLocalModels = false;

const DATA_URL = "./data/papers.json";
const MODEL_ID = "Xenova/all-MiniLM-L6-v2";
const TOP_K = 8;

const els = {
  form: document.getElementById("search-form"),
  input: document.getElementById("search-input"),
  status: document.getElementById("search-status"),
  results: document.getElementById("results"),
  meta: document.getElementById("index-meta"),
  chips: document.querySelectorAll(".chip"),
};

let extractorPromise = null;
let index = null;

function setStatus(text, busy = false) {
  els.status.textContent = text;
  els.status.classList.toggle("busy", busy);
}

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s ?? "";
  return div.innerHTML;
}

function truncate(s, n) {
  return s.length > n ? s.slice(0, n).trim() + "…" : s;
}

async function loadIndex() {
  const res = await fetch(DATA_URL, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(
      res.status === 404
        ? "index not built yet — the first scheduled GitHub Action run will populate it"
        : `failed to load search index (HTTP ${res.status})`
    );
  }
  index = await res.json();
  if (!index.papers || index.papers.length === 0) {
    throw new Error("index is empty — waiting on the next GitHub Action run");
  }
  const updated = new Date(index.generated_at).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
  els.meta.textContent = `${index.count} papers indexed · last refreshed ${updated} · category cs.AI`;
}

function loadModel() {
  if (!extractorPromise) {
    extractorPromise = pipeline("feature-extraction", MODEL_ID, {
      quantized: true,
      progress_callback: (p) => {
        if (p.status === "progress" && typeof p.progress === "number") {
          setStatus(`Downloading embedding model… ${Math.round(p.progress)}%`, true);
        }
      },
    });
  }
  return extractorPromise;
}

function cosineSim(a, b) {
  // Both vectors are pre-normalized (unit length), so the dot product
  // already equals cosine similarity — no need to divide by magnitudes.
  let dot = 0;
  for (let i = 0; i < a.length; i++) dot += a[i] * b[i];
  return dot;
}

function renderResults(query, hits) {
  els.results.innerHTML = "";
  if (!hits.length) {
    els.results.innerHTML = `<p class="empty">No matches for “${escapeHtml(query)}”. Try a broader query.</p>`;
    return;
  }
  for (const { paper, score } of hits) {
    const card = document.createElement("article");
    card.className = "card";
    const pct = Math.round(Math.max(0, Math.min(1, score)) * 100);
    const authors = paper.authors ?? [];
    const authorLine =
      authors.slice(0, 4).join(", ") + (authors.length > 4 ? ", et al." : "");
    const published = paper.published ? new Date(paper.published).toLocaleDateString() : "";

    card.innerHTML = `
      <div class="card-head">
        <h3>${escapeHtml(paper.title)}</h3>
        <span class="score" title="Cosine similarity: ${score.toFixed(3)}">${pct}% match</span>
      </div>
      <p class="authors">${escapeHtml(authorLine)}${published ? " · " + published : ""}</p>
      <p class="abstract">${escapeHtml(truncate(paper.abstract, 240))}</p>
      <a class="view-link" href="${paper.link}" target="_blank" rel="noopener">View on arXiv →</a>
    `;
    els.results.appendChild(card);
  }
}

async function runSearch(rawQuery) {
  const query = rawQuery.trim();
  if (!query || !index) return;

  try {
    setStatus("Embedding your query in-browser…", true);
    const extractor = await loadModel();
    const output = await extractor(query, { pooling: "mean", normalize: true });
    const queryVec = Array.from(output.data);

    setStatus("Ranking against the index…", true);
    const scored = index.papers
      .map((paper) => ({ paper, score: cosineSim(queryVec, paper.embedding) }))
      .sort((a, b) => b.score - a.score)
      .slice(0, TOP_K);

    renderResults(query, scored);
    setStatus(
      `Top ${scored.length} matches for “${query}” — ranked by cosine similarity, computed entirely on your device.`,
      false
    );
  } catch (err) {
    setStatus(`Search failed: ${err.message}`, false);
  }
}

els.form.addEventListener("submit", (e) => {
  e.preventDefault();
  runSearch(els.input.value);
});

els.chips.forEach((chip) => {
  chip.addEventListener("click", () => {
    els.input.value = chip.textContent;
    runSearch(chip.textContent);
  });
});

(async function init() {
  try {
    setStatus("Loading search index from GitHub Pages…", true);
    await loadIndex();
    setStatus("Ready — try a query, or pick an example below.", false);
    // Warm the model in the background so the first real search feels instant.
    loadModel()
      .then(() => setStatus("Ready — try a query, or pick an example below.", false))
      .catch(() => {});
  } catch (err) {
    setStatus(`Couldn't load the search index: ${err.message}`, false);
  }
})();
