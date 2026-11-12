const isMockMode = window.location.port === "8000" || window.location.protocol === "file:";
const STATE_URL = isMockMode ? "../mock_state.json" : "/api/state";

function escapeHtml(text) {
    const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
    return String(text).replace(/[&<>"']/g, m => map[m]);
}

async function fetchState() {
    try {
        const response = await fetch(STATE_URL);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const state = await response.json();
        renderUI(state);
    } catch (error) {
        console.error("Lỗi tải state:", error);
        const badge = document.getElementById("connection-status");
        badge.className = "status-badge status-disconnected";
        badge.innerText = "disconnected / gateway unreachable";
    }
}

function renderUI(state) {
    // 1. Trạng thái kết nối & Username
    document.getElementById("current-user").innerText = state.username || "Unknown";
    const statusObj = state.status || { state: "disconnected" };
    const statusBadge = document.getElementById("connection-status");
    statusBadge.innerText = statusObj.state + (statusObj.detail ? `: ${statusObj.detail}` : "");
    statusBadge.className = `status-badge ${statusObj.state === 'connected' ? 'status-connected' : 'status-disconnected'}`;

    // 2. Danh sách Online Users & Select Box Chat
    const users = state.users || [];
    document.getElementById("user-count").innerText = users.length;
    const usersUl = document.getElementById("users-list");
    const chatToSelect = document.getElementById("chat-to");
    const currentSelected = chatToSelect.value;

    usersUl.innerHTML = "";
    chatToSelect.innerHTML = '<option value="*">Broadcast (*)</option>';

    users.forEach(u => {
        const li = document.createElement("li");
        li.innerText = u;
        usersUl.appendChild(li);

        if (u !== state.username) {
            const opt = document.createElement("option");
            opt.value = u;
            opt.innerText = u;
            chatToSelect.appendChild(opt);
        }
    });

    if ([...chatToSelect.options].some(o => o.value === currentSelected)) {
        chatToSelect.value = currentSelected;
    }

    // 3. Khung Chat (Broadcast & Unicast)
    const chatBox = document.getElementById("chat-box");
    const messages = state.messages || [];
    const isScrolledToBottom = chatBox.scrollHeight - chatBox.clientHeight <= chatBox.scrollTop + 10;

    chatBox.innerHTML = "";
    messages.forEach(msg => {
        const div = document.createElement("div");
        div.className = `message ${msg.direction}`;
        const timeStr = new Date(msg.time * 1000).toLocaleTimeString();
        div.innerHTML = `
            <div><strong>${escapeHtml(msg.from)}</strong> ➔ <em>${escapeHtml(msg.to)}</em>: ${escapeHtml(msg.text)}</div>
            <div class="message-meta">${timeStr}</div>
        `;
        chatBox.appendChild(div);
    });

    if (isScrolledToBottom) {
        chatBox.scrollTop = chatBox.scrollHeight;
    }

    // 4. Bảng tiến độ truyền file (File Transfers)
    const tbody = document.getElementById("transfers-tbody");
    const transfers = state.transfers || [];
    if (transfers.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" style="text-align: center;">Chưa có tệp truyền nào.</td></tr>`;
    } else {
        tbody.innerHTML = "";
        transfers.forEach(t => {
            const tr = document.createElement("tr");
            const percent = Math.round((t.progress || 0) * 100);
            tr.innerHTML = `
                <td>${escapeHtml(t.peer)}</td>
                <td>${escapeHtml(t.filename)}</td>
                <td>${escapeHtml(t.direction)}</td>
                <td>
                    <div>${percent}%</div>
                    <div class="progress-bar-container">
                        <div class="progress-bar" style="width: ${percent}%"></div>
                    </div>
                </td>
                <td><strong>${escapeHtml(t.status)}</strong> ${t.error ? '(' + escapeHtml(t.error) + ')' : ''}</td>
                <td>${t.cwnd !== null ? 'cwnd: ' + t.cwnd : '-'} ${t.rto !== null ? '/ rto: ' + t.rto + 's' : ''}</td>
            `;
            tbody.appendChild(tr);
        });
    }
}

// Xử lý sự kiện gửi tin nhắn
document.getElementById("chat-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const to = document.getElementById("chat-to").value;
    const textInput = document.getElementById("chat-text");
    const text = textInput.value.trim();
    if (!text) return;

    if (!isMockMode) {
        try {
            const res = await fetch("/api/chat", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ to, text })
            });
            if (!res.ok) {
                const err = await res.json();
                alert("Lỗi gửi tin nhắn: " + (err.error || res.statusText));
                return;
            }
            textInput.value = "";
            fetchState();
        } catch (e) {
            alert("Không thể kết nối tới gateway: " + e.message);
        }
    } else {
        alert(`[Mock Mode - W6-05] Đã gửi tin nhắn tới [${to}]: "${text}"`);
        textInput.value = "";
    }
});

// Polling cập nhật trạng thái mỗi 1 giây
setInterval(fetchState, 1000);
fetchState();