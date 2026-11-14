/**
 * Networking App - Client Dashboard Application
 * Features:
 *  1. Login / Logout flow with gateway authentication
 *  2. Real-time push with Server-Sent Events (SSE) & automatic polling fallback
 *  3. P2P File Upload & Transfer with Base64 encoding
 *  4. Direct file download for completed incoming transfers
 *  5. Modern responsive UI with live status indicators
 */

// Chế độ mock tự động kích hoạt khi mở file trực tiếp hoặc cổng 8000
const isFileProtocol = window.location.protocol === "file:";
const isMockMode = isFileProtocol || window.location.port === "8000" || window.location.search.includes("mock=1");
const STATE_URL = isFileProtocol ? "../mock_state.json" : (isMockMode ? "/mock_state.json" : "/api/state");

let evtSource = null;
let pollingInterval = null;
let currentUsername = null;

// ================= UTILITIES ================= //

function escapeHtml(text) {
    if (text === null || text === undefined) return "";
    const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
    return String(text).replace(/[&<>"']/g, m => map[m]);
}

function formatBytes(bytes, decimals = 1) {
    if (!bytes || bytes === 0) return "0 B";
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

// ================= VIEW TOGGLE (LOGIN / DASHBOARD) ================= //

function showLoginView() {
    currentUsername = null;
    document.getElementById("dashboard-view").style.display = "none";
    document.getElementById("login-view").style.display = "flex";
    document.getElementById("login-username").focus();
}

function showDashboardView(username) {
    currentUsername = username;
    document.getElementById("login-view").style.display = "none";
    document.getElementById("dashboard-view").style.display = "grid";
}

function updateSyncBadge(mode, label) {
    const badge = document.getElementById("sync-status-badge");
    const text = document.getElementById("sync-status-text");
    const statSync = document.getElementById("stat-sync-mode");
    
    if (mode === "sse") {
        badge.className = "badge badge-success";
        text.innerText = label || "Real-time (SSE)";
        if (statSync) statSync.innerText = "SSE (Push)";
    } else {
        badge.className = "badge badge-warning";
        text.innerText = label || "Polling (1.5s)";
        if (statSync) statSync.innerText = "HTTP Polling";
    }
}

// ================= REAL-TIME PUSH (SSE) & POLLING ================= //

function startSSE() {
    if (isMockMode || typeof EventSource === "undefined") {
        console.log("Môi trường mock / không hỗ trợ SSE -> fallback sang Polling");
        updateSyncBadge("polling", "Polling (Mock)");
        startPolling();
        return;
    }

    stopSSE();

    try {
        evtSource = new EventSource("/api/events");

        evtSource.onopen = () => {
            console.log("SSE đã kết nối thành công");
            updateSyncBadge("sse", "Real-time (SSE)");
            stopPolling();
        };

        evtSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const state = (data && data.state) ? data.state : data;
                
                // Nếu state báo chưa đăng nhập, chuyển về login
                if (!state.username || state.username === null) {
                    handleUnauthenticated();
                    return;
                }
                renderUI(state);
            } catch (err) {
                console.error("Lỗi phân tích dữ liệu SSE:", err);
            }
        };

        evtSource.onerror = (err) => {
            console.warn("SSE bị ngắt kết nối hoặc lỗi, chuyển sang polling fallback:", err);
            updateSyncBadge("polling", "Polling Fallback (1.5s)");
            startPolling();
        };
    } catch (err) {
        console.error("Không thể khởi tạo EventSource:", err);
        updateSyncBadge("polling", "Polling Fallback (1.5s)");
        startPolling();
    }
}

function stopSSE() {
    if (evtSource) {
        evtSource.close();
        evtSource = null;
    }
}

function startPolling() {
    if (pollingInterval) return;
    pollingInterval = setInterval(fetchState, 1500);
}

function stopPolling() {
    if (pollingInterval) {
        clearInterval(pollingInterval);
        pollingInterval = null;
    }
}

function handleUnauthenticated() {
    stopSSE();
    stopPolling();
    showLoginView();
}

// ================= FETCH STATE ================= //

async function fetchState() {
    try {
        let response = await fetch(STATE_URL);
        
        // Thử dự phòng nếu đường dẫn mock khác
        if (!response.ok && isMockMode && STATE_URL === "/mock_state.json") {
            response = await fetch("mock_state.json");
        }

        if (response.status === 401) {
            handleUnauthenticated();
            return;
        }

        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const state = await response.json();

        // Xử lý mock logout/login
        if (isMockMode) {
            if (sessionStorage.getItem("mock_logged_out") === "true") {
                handleUnauthenticated();
                return;
            }
            if (sessionStorage.getItem("mock_username")) {
                state.username = sessionStorage.getItem("mock_username");
            }
        }

        // Kiểm tra xem đã đăng nhập chưa
        if (!state.username || state.username === null) {
            handleUnauthenticated();
            return;
        }

        renderUI(state);
    } catch (error) {
        console.error("Lỗi khi tải trạng thái hệ thống:", error);
        const statusBadge = document.getElementById("connection-status");
        if (statusBadge) {
            statusBadge.className = "status-badge status-disconnected";
            const textElem = statusBadge.querySelector(".status-text");
            if (textElem) textElem.innerText = "disconnected / gateway unreachable";
        }
    }
}

// ================= RENDER UI ================= //

function renderUI(state) {
    // 1. Hiển thị Dashboard nếu đang ở màn hình login
    showDashboardView(state.username);

    // 2. Thông tin User & Trạng thái kết nối
    const usernameElem = document.getElementById("current-user");
    const avatarElem = document.getElementById("user-avatar");
    const username = state.username || "Unknown";
    
    usernameElem.innerText = username;
    avatarElem.innerText = username.charAt(0).toUpperCase();

    const statusObj = state.status || { state: "disconnected" };
    const statusBadge = document.getElementById("connection-status");
    const statusTextElem = statusBadge.querySelector(".status-text");
    
    const isConnected = statusObj.state === "connected";
    statusBadge.className = `status-badge ${isConnected ? "status-connected" : "status-disconnected"}`;
    const detailStr = statusObj.detail ? `: ${statusObj.detail}` : "";
    if (statusTextElem) {
        statusTextElem.innerText = statusObj.state + detailStr;
    }

    // 3. Danh sách User Online & Đồng bộ Dropdowns
    const users = state.users || [];
    document.getElementById("user-count").innerText = users.length;
    const usersUl = document.getElementById("users-list");
    const chatToSelect = document.getElementById("chat-to");
    const fileToSelect = document.getElementById("file-to");

    const currentChatSelected = chatToSelect.value;
    const currentFileSelected = fileToSelect.value;

    usersUl.innerHTML = "";
    chatToSelect.innerHTML = '<option value="*">Broadcast (*)</option>';
    fileToSelect.innerHTML = '<option value="" disabled>-- Chọn người nhận --</option>';

    if (users.length === 0) {
        usersUl.innerHTML = '<li class="user-list-empty">Không có người dùng nào trực tuyến.</li>';
    } else {
        users.forEach(u => {
            const isMe = (u === state.username);
            const li = document.createElement("li");
            li.className = isMe ? "user-item user-me" : "user-item";
            li.innerHTML = `
                <span class="user-dot"></span>
                <span class="user-item-name">${escapeHtml(u)}</span>
                ${isMe ? '<span class="user-you-tag">(bạn)</span>' : ''}
            `;
            
            // Bấm vào tên user để tự động chọn người nhận chat/file
            if (!isMe) {
                li.style.cursor = "pointer";
                li.title = `Nhấn để gửi tin nhắn hoặc file cho ${u}`;
                li.addEventListener("click", () => {
                    chatToSelect.value = u;
                    fileToSelect.value = u;
                });
            }
            usersUl.appendChild(li);

            // Populate options cho chat & file
            if (!isMe) {
                const optChat = document.createElement("option");
                optChat.value = u;
                optChat.innerText = u;
                chatToSelect.appendChild(optChat);

                const optFile = document.createElement("option");
                optFile.value = u;
                optFile.innerText = u;
                fileToSelect.appendChild(optFile);
            }
        });
    }

    // Khôi phục giá trị đã chọn nếu còn hợp lệ
    if ([...chatToSelect.options].some(o => o.value === currentChatSelected)) {
        chatToSelect.value = currentChatSelected;
    }
    if ([...fileToSelect.options].some(o => o.value === currentFileSelected)) {
        fileToSelect.value = currentFileSelected;
    } else if (fileToSelect.options.length > 1 && !currentFileSelected) {
        // Chọn mặc định peer đầu tiên nếu chưa chọn
        fileToSelect.selectedIndex = 1;
    }

    // 4. Khung Chat (Broadcast & Unicast)
    const chatBox = document.getElementById("chat-box");
    const messages = state.messages || [];
    const isScrolledToBottom = chatBox.scrollHeight - chatBox.clientHeight <= chatBox.scrollTop + 20;

    chatBox.innerHTML = "";
    if (messages.length === 0) {
        chatBox.innerHTML = '<div class="chat-empty">Chưa có tin nhắn nào. Hãy bắt đầu trò chuyện!</div>';
    } else {
        messages.forEach(msg => {
            const div = document.createElement("div");
            const isOutgoing = msg.direction === "outgoing" || msg.from === state.username;
            div.className = `message ${isOutgoing ? "outgoing" : "incoming"}`;
            
            const timeStr = msg.time ? new Date(msg.time * 1000).toLocaleTimeString() : "";
            const isBroadcast = msg.to === "*";
            const toBadge = isBroadcast 
                ? '<span class="msg-to-badge msg-to-broadcast">Broadcast</span>' 
                : `<span class="msg-to-badge">đến ${escapeHtml(msg.to)}</span>`;

            div.innerHTML = `
                <div class="message-header">
                    <strong class="msg-sender">${escapeHtml(msg.from)}</strong>
                    ${toBadge}
                </div>
                <div class="message-content">${escapeHtml(msg.text)}</div>
                <div class="message-meta">${timeStr}</div>
            `;
            chatBox.appendChild(div);
        });
    }

    if (isScrolledToBottom) {
        chatBox.scrollTop = chatBox.scrollHeight;
    }

    // 5. Thống kê nhanh
    const msgTotalElem = document.getElementById("stat-msg-total");
    if (msgTotalElem) msgTotalElem.innerText = `${messages.length} tin`;

    // 6. Bảng danh sách truyền tệp (File Transfers)
    const tbody = document.getElementById("transfers-tbody");
    const transfers = state.transfers || [];
    
    const transferTotalElem = document.getElementById("stat-transfer-total");
    if (transferTotalElem) transferTotalElem.innerText = `${transfers.length} tệp`;

    if (transfers.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" class="table-empty">Chưa có tệp truyền nào.</td></tr>`;
    } else {
        tbody.innerHTML = "";
        transfers.forEach(t => {
            const tr = document.createElement("tr");
            const percent = Math.min(100, Math.max(0, Math.round((t.progress || 0) * 100)));
            const isIncoming = t.direction === "incoming" || t.direction === "receive";
            const isCompleted = t.status === "completed";
            const isFailed = t.status === "failed";
            const isTransferring = t.status === "transferring";

            // Status Badge
            let statusBadgeClass = "badge-status-neutral";
            if (isCompleted) statusBadgeClass = "badge-status-completed";
            else if (isFailed) statusBadgeClass = "badge-status-failed";
            else if (isTransferring) statusBadgeClass = "badge-status-transferring";

            // Direction Badge
            const dirBadge = isIncoming
                ? `<span class="badge-direction badge-incoming">📥 Nhận</span>`
                : `<span class="badge-direction badge-outgoing">📤 Gửi</span>`;

            // Nút Tải file cho tệp nhận đã hoàn tất
            let actionHtml = `<span class="text-muted">-</span>`;
            if (isIncoming && isCompleted) {
                const downloadUrl = `/api/file/download?filename=${encodeURIComponent(t.filename)}`;
                actionHtml = `
                    <a href="${downloadUrl}" class="btn-download" download="${escapeHtml(t.filename)}" title="Tải tệp tin về máy">
                        ⬇️ Tải về
                    </a>
                `;
            }

            // Dung lượng tệp
            const sizeStr = t.size ? ` (${formatBytes(t.bytes_transferred || 0)} / ${formatBytes(t.size)})` : "";

            tr.innerHTML = `
                <td><strong>${escapeHtml(t.peer)}</strong></td>
                <td>
                    <div class="file-name-cell" title="${escapeHtml(t.filename)}">
                        📄 ${escapeHtml(t.filename)}
                        <small class="text-muted">${sizeStr}</small>
                    </div>
                </td>
                <td>${dirBadge}</td>
                <td style="min-width: 140px;">
                    <div class="progress-cell">
                        <div class="progress-bar-container">
                            <div class="progress-bar ${isCompleted ? 'progress-completed' : (isFailed ? 'progress-failed' : '')}" style="width: ${percent}%;"></div>
                        </div>
                        <span class="progress-num">${percent}%</span>
                    </div>
                </td>
                <td>
                    <span class="badge ${statusBadgeClass}">
                        ${escapeHtml(t.status)}
                    </span>
                    ${t.error ? `<div class="transfer-error" title="${escapeHtml(t.error)}">${escapeHtml(t.error)}</div>` : ''}
                </td>
                <td>
                    <div class="congestion-info">
                        ${t.cwnd !== null && t.cwnd !== undefined ? `<span>cwnd: <strong>${t.cwnd}</strong></span>` : '-'}
                        ${t.rto !== null && t.rto !== undefined ? `<br><span>rto: <strong>${t.rto}s</strong></span>` : ''}
                    </div>
                </td>
                <td class="action-cell">${actionHtml}</td>
            `;
            tbody.appendChild(tr);
        });
    }
}

// ================= EVENT LISTENERS ================= //

// 1. Submit Form Đăng nhập
document.getElementById("login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const usernameInput = document.getElementById("login-username");
    const passwordInput = document.getElementById("login-password");
    const errorElem = document.getElementById("login-error");
    const loginBtn = document.getElementById("login-btn");

    const username = usernameInput.value.trim();
    const password = passwordInput.value;

    errorElem.style.display = "none";
    errorElem.innerText = "";

    if (!username) {
        errorElem.innerText = "Vui lòng nhập tên đăng nhập!";
        errorElem.style.display = "block";
        return;
    }

    loginBtn.disabled = true;
    loginBtn.classList.add("loading");

    // Xử lý chế độ Mock
    if (isMockMode) {
        setTimeout(() => {
            sessionStorage.removeItem("mock_logged_out");
            sessionStorage.setItem("mock_username", username);
            loginBtn.disabled = false;
            loginBtn.classList.remove("loading");
            passwordInput.value = "";
            fetchState();
            startSSE();
        }, 300);
        return;
    }

    // Gửi yêu cầu đăng nhập thực tế tới gateway
    try {
        const response = await fetch("/api/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username, password })
        });

        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.error || `Đăng nhập không thành công (HTTP ${response.status})`);
        }

        loginBtn.disabled = false;
        loginBtn.classList.remove("loading");
        passwordInput.value = "";

        // Khi đăng nhập thành công: render dashboard và kết nối SSE
        await fetchState();
        startSSE();
    } catch (err) {
        loginBtn.disabled = false;
        loginBtn.classList.remove("loading");
        errorElem.innerText = err.message || "Không thể kết nối tới gateway!";
        errorElem.style.display = "block";
    }
});

// 2. Nút Đăng xuất (Logout)
document.getElementById("btn-logout").addEventListener("click", async () => {
    if (isMockMode) {
        sessionStorage.setItem("mock_logged_out", "true");
        sessionStorage.removeItem("mock_username");
        handleUnauthenticated();
        return;
    }

    try {
        await fetch("/api/logout", {
            method: "POST",
            headers: { "Content-Type": "application/json" }
        });
    } catch (err) {
        console.warn("Lỗi gửi yêu cầu logout:", err);
    } finally {
        handleUnauthenticated();
    }
});

// 3. Gửi Tin Nhắn (Chat Form)
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
                const err = await res.json().catch(() => ({}));
                alert("Lỗi gửi tin nhắn: " + (err.error || res.statusText));
                return;
            }
            textInput.value = "";
            fetchState();
        } catch (e) {
            alert("Không thể kết nối tới gateway: " + e.message);
        }
    } else {
        alert(`[Mock Mode] Đã gửi tin nhắn tới [${to}]: "${text}"`);
        textInput.value = "";
    }
});

// 4. Quản lý Chọn & Gửi File (File Transfer)
const fileInput = document.getElementById("file-input");
const btnBrowseFile = document.getElementById("btn-browse-file");
const fileSelectionInfo = document.getElementById("file-selection-info");
const fileNameDisplay = document.getElementById("file-name-display");
const fileSizeDisplay = document.getElementById("file-size-display");
const btnClearFile = document.getElementById("btn-clear-file");
const fileStatusBox = document.getElementById("file-status");

function resetFileSelection() {
    fileInput.value = "";
    fileSelectionInfo.style.display = "none";
    btnBrowseFile.style.display = "inline-flex";
}

function showFileStatus(msg, type = "info") {
    fileStatusBox.style.display = "block";
    fileStatusBox.className = `file-status-box status-${type}`;
    let icon = "ℹ️";
    if (type === "success") icon = "✅";
    if (type === "error") icon = "❌";
    if (type === "loading") icon = "⏳";
    fileStatusBox.innerHTML = `<span>${icon} ${escapeHtml(msg)}</span>`;
}

function hideFileStatus() {
    fileStatusBox.style.display = "none";
    fileStatusBox.innerHTML = "";
}

btnBrowseFile.addEventListener("click", () => fileInput.click());

fileInput.addEventListener("change", () => {
    const file = fileInput.files[0];
    if (file) {
        fileNameDisplay.innerText = file.name;
        fileNameDisplay.title = file.name;
        fileSizeDisplay.innerText = `(${formatBytes(file.size)})`;
        fileSelectionInfo.style.display = "inline-flex";
        btnBrowseFile.style.display = "none";
        hideFileStatus();
    } else {
        resetFileSelection();
    }
});

btnClearFile.addEventListener("click", () => {
    resetFileSelection();
    hideFileStatus();
});

// Gửi file khi submit form
document.getElementById("file-upload-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const toUser = document.getElementById("file-to").value;
    const file = fileInput.files[0];

    if (!toUser) {
        showFileStatus("Vui lòng chọn người nhận (recipient)!", "error");
        return;
    }

    if (!file) {
        showFileStatus("Vui lòng chọn một tệp tin để gửi!", "error");
        return;
    }

    showFileStatus(`Đang đọc tệp "${file.name}"...`, "loading");

    const reader = new FileReader();

    reader.onload = async () => {
        try {
            const dataUrl = reader.result;
            // Cắt bỏ prefix "data:...;base64," để lấy Base64 thuần túy
            const base64Data = dataUrl.includes(",") ? dataUrl.split(",")[1] : dataUrl;

            showFileStatus(`Đang truyền tệp "${file.name}" tới ${toUser}...`, "loading");

            if (isMockMode) {
                setTimeout(() => {
                    showFileStatus(`[Mock Mode] Gửi file "${file.name}" (${formatBytes(file.size)}) tới ${toUser} thành công!`, "success");
                    resetFileSelection();
                }, 700);
                return;
            }

            const response = await fetch("/api/file/upload", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    to: toUser,
                    filename: file.name,
                    content: base64Data
                })
            });

            if (!response.ok) {
                const errData = await response.json().catch(() => ({}));
                throw new Error(errData.error || `HTTP ${response.status}`);
            }

            showFileStatus(`Gửi file "${file.name}" thành công!`, "success");
            resetFileSelection();
            fetchState();
        } catch (err) {
            showFileStatus(`Lỗi gửi file: ${err.message}`, "error");
        }
    };

    reader.onerror = () => {
        showFileStatus("Không thể đọc tệp tin từ hệ thống cục bộ!", "error");
    };

    reader.readAsDataURL(file);
});

// ================= KHỞI ĐỘNG ỨNG DỤNG ================= //

async function initApp() {
    await fetchState();
    // Bắt đầu lắng nghe SSE ngay khi vào ứng dụng
    startSSE();
}

// Chạy khởi tạo ứng dụng
initApp();
