const apiGet = async (url) => {
  const res = await fetch(url, { credentials: "include" });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

const apiPost = async (url, data = {}) => {
  const body = new URLSearchParams();
  Object.entries(data).forEach(([k, v]) => body.append(k, v));
  const res = await fetch(url, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
};

const qs = (sel) => document.querySelector(sel);
const qsa = (sel) => Array.from(document.querySelectorAll(sel));

const bindText = (sel, text) => {
  const el = qs(sel);
  if (el) el.textContent = text ?? "";
};

const page = document.body.dataset.page;

async function initLogin() {
  const form = qs("#login-form");
  const err = qs("#login-error");
  if (!form) return;
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (err) err.style.display = "none";
    const fd = new FormData(form);
    try {
      const out = await apiPost("/api/login", {
        username: fd.get("username") || "",
        password: fd.get("password") || "",
      });
      if (out.ok) {
        window.location.href = "/threads";
      } else if (err) {
        err.textContent = out.error || "Ошибка входа";
        err.style.display = "block";
      }
    } catch (ex) {
      if (err) {
        err.textContent = "Ошибка входа";
        err.style.display = "block";
      }
    }
  });
}

async function initHome() {
  const data = await apiGet("/api/me");
  bindText("[data-bind=who]", data.who || "пользователь");
}

function renderParserStatus(container, status) {
  if (!container) return;
  container.querySelector("[data-bind=status-text]").textContent = status.text;
  const badge = container.querySelector("[data-bind=status-text]");
  badge.classList.remove("ok", "bad", "idle");
  badge.classList.add(status.color);

  const postsBadge = container.querySelector("[data-bind=posts-ok]");
  postsBadge.textContent = status.posts_ok ? "OK" : "Проблема";
  postsBadge.classList.remove("ok", "bad");
  postsBadge.classList.add(status.posts_ok ? "ok" : "bad");

  const accBadge = container.querySelector("[data-bind=accounts-ok]");
  accBadge.textContent = status.accounts_ok ? "OK" : "Проблема";
  accBadge.classList.remove("ok", "bad");
  accBadge.classList.add(status.accounts_ok ? "ok" : "bad");
}

async function initThreadsHome() {
  const data = await apiGet("/api/threads");
  bindText("[data-bind=who]", data.who);
  renderParserStatus(qs("#parser-status"), data.parser_status);
}

async function initThreadsAccounts() {
  const data = await apiGet("/api/threads/accounts");
  bindText("[data-bind=who]", data.who);
  const list = qs("#accounts-list");
  list.innerHTML = "";
  if (!data.accounts || data.accounts.length === 0) {
    list.innerHTML = '<div class="empty">Пока пусто.</div>';
  } else {
    data.accounts.forEach((item) => {
      const norm = item;
      const autoOn = data.auto_add && data.auto_add[norm] !== undefined ? data.auto_add[norm] : true;
      const li = document.createElement("li");
      li.className = "chip";
      li.innerHTML = `
        <span>${item}</span>
        <div class="chip-actions">
          <label class="chip-link checkbox-label">
            <input type="checkbox" ${autoOn ? "checked" : ""} data-auto="${item}">
            Авто-пост
          </label>
          <a class="chip-link" href="/threads/accounts/stats?item=${encodeURIComponent(item)}">Статистика</a>
          <button data-remove="${item}" aria-label="Удалить аккаунт">×</button>
        </div>
      `;
      list.appendChild(li);
    });
  }

  qs("#account-add-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = qs("#account-input");
    await apiPost("/api/threads/accounts/add", { account: input.value });
    input.value = "";
    initThreadsAccounts();
  });

  list.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-remove]");
    if (btn) {
      await apiPost("/api/threads/accounts/remove", { account: btn.dataset.remove });
      initThreadsAccounts();
    }
  });

  list.addEventListener("change", async (e) => {
    const cb = e.target.closest("input[data-auto]");
    if (cb) {
      await apiPost("/api/threads/accounts/auto_toggle", {
        account: cb.dataset.auto,
        enabled: cb.checked ? "1" : "0",
      });
    }
  });
}

async function initThreadsPosts() {
  const data = await apiGet("/api/threads/posts");
  bindText("[data-bind=who]", data.who);
  const list = qs("#posts-list");
  list.innerHTML = "";
  if (!data.posts || data.posts.length === 0) {
    list.innerHTML = '<div class="empty">Пока пусто.</div>';
  } else {
    data.posts.forEach((item) => {
      const li = document.createElement("li");
      li.className = "chip";
      li.innerHTML = `
        <span>${item}</span>
        <div class="chip-actions">
          <a class="chip-link" href="/threads/posts/stats?item=${encodeURIComponent(item)}">Статистика</a>
          <button data-remove="${item}" aria-label="Удалить пост">×</button>
        </div>
      `;
      list.appendChild(li);
    });
  }

  qs("#post-add-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = qs("#post-input");
    await apiPost("/api/threads/posts/add", { post: input.value });
    input.value = "";
    initThreadsPosts();
  });

  list.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-remove]");
    if (btn) {
      await apiPost("/api/threads/posts/remove", { post: btn.dataset.remove });
      initThreadsPosts();
    }
  });
}

async function initThreadsPostStats() {
  const url = new URL(window.location.href);
  const item = url.searchParams.get("item") || "";
  const data = await apiGet(`/api/threads/posts/stats?item=${encodeURIComponent(item)}`);
  bindText("[data-bind=who]", data.who);
  bindText("#post-item", data.item);
  const statsDate = data.stats?.dateTime;
  const hasStatsDate =
    !!(
      statsDate &&
      typeof statsDate === "object" &&
      (statsDate.msk_human || statsDate.msk || statsDate.raw)
    ) ||
    (typeof statsDate === "string" && statsDate.trim().length > 0);
  const postDate = hasStatsDate ? statsDate : (data.post_date || {});
  const postDateText =
    (typeof postDate === "object"
      ? (postDate.msk_human || postDate.msk || postDate.raw || "")
      : String(postDate || "")) || "—";
  bindText("#post-date", postDateText);
  bindText("#stat-views", data.stats?.views ?? "—");
  bindText("#stat-likes", data.stats?.likes ?? "—");
  bindText("#stat-comments", data.stats?.comments ?? "—");
  bindText("#stat-repost", data.stats?.repost ?? "—");
  bindText("#stat-shared", data.stats?.shared ?? "—");

  const selector = qs("#metric-select");
  const grids = qsa(".dyn-grid");
  const metrics = ["views", "likes", "comments", "repost", "shared"];
  const labels = {
    views: "Просмотры",
    likes: "Лайки",
    comments: "Комментарии",
    repost: "Репосты",
    shared: "Поделились",
  };

  selector.innerHTML = metrics.map((m) => `<option value="${m}">${labels[m]}</option>`).join("");

  const renderGrid = (metric) => {
    grids.forEach((g) => (g.innerHTML = ""));
    const cards = data.dynamics?.[metric] || [];
    const target = qs(`#dyn-${metric}`);
    target.innerHTML = cards
      .map(
        (card) => `
      <div class="stat-card">
        <h4>${card.label}</h4>
        <span>${card.delta}</span>
      </div>`
      )
      .join("");
  };

  selector.addEventListener("change", () => {
    const metric = selector.value;
    grids.forEach((g) => g.classList.add("hidden"));
    qs(`#dyn-${metric}`).classList.remove("hidden");
    renderGrid(metric);
  });

  grids.forEach((g) => g.classList.add("hidden"));
  qs(`#dyn-views`).classList.remove("hidden");
  renderGrid("views");

  const commentsBlock = qs("#comments-block");
  if (commentsBlock) {
    const threads = data.stats?.comments_threads || [];
    const list = data.stats?.comments_list || [];
    if (Array.isArray(threads) && threads.length) {
      commentsBlock.innerHTML = threads
        .map((thread, idx) => {
          const items = (thread || [])
            .map(
              (c) => `
              <div class="post-item">
                <div class="post-meta">
                  <strong>${c.username || "—"}</strong>
                  <small>${c.text || ""}</small>
                </div>
              </div>`
            )
            .join("");
          return `
            <div class="post-item thread-head">
              <div class="post-meta">
                <strong>Ветка ${idx + 1}</strong>
                <small>${(thread || []).length} комментариев</small>
              </div>
            </div>
            <div class="post-list thread-body">
              ${items}
            </div>`;
        })
        .join("");
    } else if (Array.isArray(list) && list.length) {
      commentsBlock.innerHTML = list
        .map(
          (c) => `
          <div class="post-item">
            <div class="post-meta">
              <strong>${c.username || "—"}</strong>
              <small>${c.text || ""}</small>
            </div>
          </div>`
        )
        .join("");
    } else {
      commentsBlock.innerHTML = '<div class="empty">Пока нет комментариев.</div>';
    }
  }
}

async function initThreadsParsers() {
  const data = await apiGet("/api/threads/parsers");
  bindText("[data-bind=who]", data.who);
  bindText("#status-posts", data.status_posts ? "Работает" : "Не работает");
  bindText("#status-accounts", data.status_accounts ? "Работает" : "Не работает");
  renderParserStatus(qs("#parser-status"), data.parser_status);

  const postsBadge = qs("#status-posts");
  if (postsBadge) {
    postsBadge.classList.remove("ok", "bad");
    postsBadge.classList.add(data.status_posts ? "ok" : "bad");
  }
  const accountsBadge = qs("#status-accounts");
  if (accountsBadge) {
    accountsBadge.classList.remove("ok", "bad");
    accountsBadge.classList.add(data.status_accounts ? "ok" : "bad");
  }

  qsa("[data-action]").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      await apiPost("/api/threads/parsers/control", { action: btn.dataset.action });
      initThreadsParsers();
    });
  });
}

async function initThreadsHistory() {
  const data = await apiGet("/api/threads/history");
  bindText("[data-bind=who]", data.who);
  const list = qs("#history-list");
  list.innerHTML = "";
  if (!data.items || data.items.length === 0) {
    list.innerHTML = '<div class="empty">Пока нет истории.</div>';
    return;
  }
  data.items.forEach((item) => {
    const div = document.createElement("div");
    div.className = "post-item";
    div.innerHTML = `
      <div class="post-meta">
        <a href="${item.url}" target="_blank" rel="noopener">${item.url}</a>
        <small>Дата публикации: ${item.post_date || "—"}</small>
        <small>Старт трекинга: ${item.started_at_human || "—"}</small>
        <small>Завершено: ${item.completed_at_human || "—"}</small>
      </div>
      <div style="display:flex; gap:8px; align-items:center;">
        <a class="btn" href="/threads/history/post?item=${encodeURIComponent(item.url)}">Открыть</a>
        <button class="btn" data-delete="${item.url}">×</button>
      </div>
    `;
    list.appendChild(div);
  });

  list.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-delete]");
    if (btn) {
      if (!confirm("Удалить пост и всю историю?")) return;
      await apiPost("/api/threads/history/delete", { item: btn.dataset.delete });
      initThreadsHistory();
    }
  });
}

async function initThreadsHistoryPost() {
  const url = new URL(window.location.href);
  const item = url.searchParams.get("item") || "";
  const data = await apiGet(`/api/threads/history/post?item=${encodeURIComponent(item)}`);
  bindText("[data-bind=who]", data.who);
  bindText("#history-post-item", data.item);
  bindText("#history-post-date", data.post_date || "—");
  bindText("#history-track-start", data.started_at_human || "—");
  bindText("#history-track-complete", data.completed_at_human || "—");

  const metricSelect = qs("#history-metric-select");
  const hourSelect = qs("#history-hour-select");
  const delta = qs("#history-hour-delta");
  const empty = qs("#history-empty");

  const options = Array.isArray(data.metric_options) ? data.metric_options : [];
  const byMetric = data.hourly_by_metric || {};
  metricSelect.innerHTML = options
    .map((opt) => `<option value="${opt.key}">${opt.label}</option>`)
    .join("");

  const fillHours = (metric) => {
    const rows = Array.isArray(byMetric[metric]) ? byMetric[metric] : [];
    hourSelect.innerHTML = rows
      .map(
        (row, idx) =>
          `<option value="${idx}" data-delta="${row.delta ?? 0}">${row.range || "—"}</option>`
      )
      .join("");
    if (!rows.length) {
      delta.textContent = "0";
      empty.style.display = "block";
      return;
    }
    empty.style.display = "none";
    const option = hourSelect.options[hourSelect.selectedIndex];
    delta.textContent = option ? option.dataset.delta || "0" : "0";
  };

  metricSelect.addEventListener("change", () => fillHours(metricSelect.value));
  hourSelect.addEventListener("change", () => {
    const option = hourSelect.options[hourSelect.selectedIndex];
    delta.textContent = option ? option.dataset.delta || "0" : "0";
  });

  if (options.length) {
    fillHours(options[0].key);
  } else {
    fillHours("");
  }
}

async function initThreadsAccountStats() {
  const url = new URL(window.location.href);
  const item = url.searchParams.get("item") || "";
  const data = await apiGet(`/api/threads/accounts/stats?item=${encodeURIComponent(item)}`);
  bindText("[data-bind=who]", data.who);
  bindText("#account-item", data.item);
  bindText("#followers-count", data.stats?.followers ?? "—");
  bindText("#found-posts-count", data.posts?.length ?? 0);

  const latestBox = qs("#latest-post");
  if (data.latest_post?.url) {
    latestBox.innerHTML = `<a href="${data.latest_post.url}" target="_blank" rel="noopener">${data.latest_post.url}</a>`;
  } else {
    latestBox.textContent = "—";
  }

  const latestTime = data.latest_post?.dateTime || {};
  bindText(
    "#latest-post-time",
    latestTime.msk_human || latestTime.msk || latestTime.raw || "—"
  );

  const dynGrid = qs("#followers-dynamics");
  dynGrid.innerHTML = "";
  const dynamics = data.followers_dynamics || [];
  if (!dynamics.length) {
    dynGrid.innerHTML = '<div class="empty">Недостаточно данных.</div>';
  } else {
    dynamics.forEach((it) => {
      const card = document.createElement("div");
      card.className = "stat-card";
      card.innerHTML = `<h4>За последние ${it.label}</h4><span>${it.delta ?? 0}</span>`;
      dynGrid.appendChild(card);
    });
  }

  const since = qs("#followers-since-post");
  if (data.followers_since_post?.delta !== undefined) {
    since.innerHTML = `<span>${data.followers_since_post.delta}</span>` +
      (data.followers_since_post.post_url ? `<small>${data.followers_since_post.post_url}</small>` : "");
  } else {
    since.textContent = "—";
  }

  const postsList = qs("#account-posts-list");
  postsList.innerHTML = "";
  const posts = data.posts || [];
  if (!posts.length) {
    postsList.innerHTML = '<div class="empty">Пока пусто.</div>';
  } else {
    posts.forEach((post) => {
      const itemEl = document.createElement("div");
      itemEl.className = "post-item";
      const dt = post.dateTime || {};
      const canTrack = !data.tracked_posts?.includes(post.url);
      itemEl.innerHTML = `
        <div class="post-meta">
          <a href="${post.url}" target="_blank" rel="noopener">${post.url}</a>
          <small>Время: ${dt.msk_human || dt.msk || dt.raw || "—"}</small>
        </div>
        <div style="display:flex; gap:8px; align-items:center;">
          ${canTrack ? `<button class="btn" data-track="${post.url}">Отслеживать статистику</button>` : ""}
        </div>
      `;
      postsList.appendChild(itemEl);
    });
  }

  postsList.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-track]");
    if (btn) {
      await apiPost("/api/threads/posts/add", { post: btn.dataset.track, next: `/threads/accounts/stats?item=${encodeURIComponent(item)}` });
      initThreadsAccountStats();
    }
  });
}

(async () => {
  try {
    if (page === "login") return initLogin();
    if (page === "home") return initHome();
    if (page === "threads") return initThreadsHome();
    if (page === "threads-accounts") return initThreadsAccounts();
    if (page === "threads-posts") return initThreadsPosts();
    if (page === "threads-post-stats") return initThreadsPostStats();
    if (page === "threads-account-stats") return initThreadsAccountStats();
    if (page === "threads-history") return initThreadsHistory();
    if (page === "threads-history-post") return initThreadsHistoryPost();
    if (page === "threads-parsers") return initThreadsParsers();
  } catch (err) {
    console.error(err);
  }
})();

