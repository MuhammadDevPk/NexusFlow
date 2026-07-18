// Live charts and polling controllers
let throughputChart;
let lastAckedCount = 0;
let lastCheckTime = Date.now();
const chartHistoryLength = 20;

// Initialize Dashboard
document.addEventListener("DOMContentLoaded", () => {
    generateUUID();
    updateJobNames();
    initChart();
    
    // Initial fetches
    fetchMetrics();
    fetchDLQ();
    fetchLogs();
    
    // Polling Intervals
    setInterval(fetchMetrics, 1000);
    setInterval(fetchDLQ, 2000);
    setInterval(fetchLogs, 1500);

    // Setup Reset Buttons
    document.getElementById('btn-reset-metrics').addEventListener('click', resetMetrics);
});

// -------------------------------------------------------------
// Throughput Graph (Chart.js)
// -------------------------------------------------------------
function initChart() {
    const ctx = document.getElementById('throughputChart').getContext('2d');
    
    // Generate initial zero data
    const labels = Array(chartHistoryLength).fill('');
    const data = Array(chartHistoryLength).fill(0);

    throughputChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Throughput',
                data: data,
                borderColor: '#4f46e5',
                backgroundColor: 'rgba(79, 70, 229, 0.05)',
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 0,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { display: false, drawBorder: false },
                    ticks: { display: false }
                },
                y: {
                    min: 0,
                    suggestedMax: 10,
                    grid: { color: 'rgba(15, 23, 42, 0.05)', drawBorder: false },
                    ticks: { color: '#475569', font: { family: 'Outfit', size: 10 } }
                }
            }
        }
    });
}

function updateChart(rate) {
    const timeLabel = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    
    throughputChart.data.labels.push(timeLabel);
    throughputChart.data.datasets[0].data.push(rate);
    
    if (throughputChart.data.labels.length > chartHistoryLength) {
        throughputChart.data.labels.shift();
        throughputChart.data.datasets[0].data.shift();
    }
    
    throughputChart.update('none'); // Update without transition for speed
}

// -------------------------------------------------------------
// Metrics & Queues Polling
// -------------------------------------------------------------
async function fetchMetrics() {
    try {
        const response = await fetch('/api/metrics');
        if (!response.ok) throw new Error('HTTP failure');
        
        const data = await response.json();
        document.getElementById('server-status').className = 'status-indicator connected';
        document.getElementById('server-status').querySelector('.status-text').textContent = 'SYSTEM ONLINE';

        // Update Stat Cards
        document.getElementById('stat-pushed').textContent = data.throughput.pushed.toLocaleString();
        document.getElementById('stat-acked').textContent = data.throughput.acked.toLocaleString();
        document.getElementById('stat-delayed').textContent = data.delayed_count.toLocaleString();
        document.getElementById('stat-dlq').textContent = data.dlq_count.toLocaleString();

        // Calculate Rate (Events per second)
        const now = Date.now();
        const deltaSec = (now - lastCheckTime) / 1000;
        if (deltaSec > 0.5) { // Protect from division issues
            const ackedDelta = data.throughput.acked - lastAckedCount;
            // Prevent negative rate on reset
            const rate = ackedDelta >= 0 ? ackedDelta / deltaSec : 0;
            updateChart(parseFloat(rate.toFixed(1)));
            
            lastAckedCount = data.throughput.acked;
            lastCheckTime = now;
        }

        // Render Active Queues
        renderQueues(data.queues, data.workers);

        // Render Workers
        renderWorkers(data.workers);

    } catch (error) {
        document.getElementById('server-status').className = 'status-indicator disconnected';
        document.getElementById('server-status').querySelector('.status-text').textContent = 'CONNECTION ERROR';
    }
}

function renderQueues(queues, workers) {
    const container = document.getElementById('queues-container');
    container.innerHTML = '';

    for (const [name, info] of Object.entries(queues)) {
        // Count workers allocated to this queue
        const activeWorkers = workers.filter(w => w.queue === name).length;
        
        // Calculate load percent (cap at 100)
        const loadThreshold = 20; // 20 events = 100% capacity
        const totalLoad = info.backlog + info.pending;
        const loadPercent = Math.min(100, (totalLoad / loadThreshold) * 100);

        const card = document.createElement('div');
        card.className = 'queue-card';
        card.innerHTML = `
            <div class="queue-header">
                <span class="queue-name">${name}</span>
                <span class="badge">${activeWorkers} workers active</span>
            </div>
            <div class="queue-stats-row">
                <div class="queue-stat-item">
                    <span>Backlog:</span>
                    <span>${info.backlog}</span>
                </div>
                <div class="queue-stat-item">
                    <span>In-Flight:</span>
                    <span>${info.pending}</span>
                </div>
                <div class="queue-stat-item">
                    <span>Scale Range:</span>
                    <span>${info.min_workers}-${info.max_workers}</span>
                </div>
            </div>
            <div class="queue-load-meter">
                <div class="queue-load-fill" style="width: ${loadPercent}%"></div>
            </div>
        `;
        container.appendChild(card);
    }
}

function renderWorkers(workers) {
    const grid = document.getElementById('workers-grid');
    grid.innerHTML = '';

    if (workers.length === 0) {
        grid.innerHTML = '<div style="color: var(--text-dark); font-size: 0.85rem; padding: 1rem;">No active worker threads reported. Start the daemon.</div>';
        return;
    }

    workers.forEach(worker => {
        const div = document.createElement('div');
        div.className = `worker-badge ${worker.status === 'busy' ? 'busy' : 'idle'}`;
        div.innerHTML = `
            <div class="worker-id">${worker.id.replace('worker:', '')}</div>
            <div class="worker-meta">Queue: ${worker.queue}</div>
            <div class="worker-meta">PID: ${worker.pid}</div>
            <div class="worker-status-tag">
                <span style="display:inline-block; width:6px; height:6px; border-radius:50%; background-color:${worker.status === 'busy' ? 'var(--success)' : 'var(--info)'}"></span>
                ${worker.status}
            </div>
        `;
        grid.appendChild(div);
    });
}

// -------------------------------------------------------------
// Logs Telemetry Polling
// -------------------------------------------------------------
let previousLogContent = "";
async function fetchLogs() {
    try {
        const response = await fetch('/api/logs');
        if (!response.ok) return;
        const data = await response.json();
        
        const logContent = data.logs.join("\n");
        if (logContent === previousLogContent) return; // Save reflows if logs unchanged
        previousLogContent = logContent;

        const terminal = document.getElementById('logs-terminal');
        terminal.innerHTML = '';

        data.logs.forEach(line => {
            if (!line.trim()) return;
            
            const logLine = document.createElement('div');
            logLine.className = 'log-line';
            
            // Format log colors
            if (line.includes('[ERROR]')) {
                logLine.classList.add('error');
            } else if (line.includes('[WARNING]')) {
                logLine.classList.add('warning');
            } else if (line.includes('[SUCCESS]')) {
                logLine.classList.add('success');
            } else if (line.includes('[INFO]')) {
                logLine.classList.add('info');
            }
            
            logLine.textContent = line;
            terminal.appendChild(logLine);
        });

        // Scroll to bottom
        terminal.scrollTop = terminal.scrollHeight;
    } catch (e) {
        // Suppress log polling errors
    }
}

// -------------------------------------------------------------
// Dead Letter Queue (DLQ) Fetch & Actions
// -------------------------------------------------------------
async function fetchDLQ() {
    try {
        const response = await fetch('/api/dlq');
        if (!response.ok) return;
        const jobs = await response.json();

        const tbody = document.getElementById('dlq-table-body');
        tbody.innerHTML = '';

        if (jobs.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="table-empty">Dead Letter Queue is empty. No failures reported.</td>
                </tr>
            `;
            return;
        }

        jobs.forEach(job => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="dlq-job-id">${job.id}</td>
                <td><span class="badge">${job.queue}</span></td>
                <td><code>${job.name}</code></td>
                <td style="font-weight:700; color:var(--warning)">${job.attempts}</td>
                <td class="dlq-error-msg">${job.lastError || 'Unknown Error'}</td>
                <td>
                    <div class="btn-group">
                        <button onclick="retryDLQJob('${job.id}')" class="btn btn-success btn-sm">Retry</button>
                        <button onclick="purgeDLQJob('${job.id}')" class="btn btn-danger btn-sm">Purge</button>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) {
        // Suppress DLQ polling errors
    }
}

async function retryDLQJob(jobId) {
    try {
        const response = await fetch('/api/dlq/retry', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: jobId })
        });
        if (response.ok) fetchDLQ();
    } catch (e) {
        console.error(e);
    }
}

async function purgeDLQJob(jobId) {
    try {
        const response = await fetch('/api/dlq/purge', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: jobId })
        });
        if (response.ok) fetchDLQ();
    } catch (e) {
        console.error(e);
    }
}

async function retryDLQAll() {
    const btn = document.getElementById('btn-dlq-retry');
    btn.disabled = true;
    try {
        await fetch('/api/dlq/retry', { method: 'POST' });
        fetchDLQ();
    } catch (e) {
        console.error(e);
    } finally {
        btn.disabled = false;
    }
}

async function purgeDLQAll() {
    if (!confirm('Are you sure you want to clear the Dead Letter Queue?')) return;
    const btn = document.getElementById('btn-dlq-purge');
    btn.disabled = true;
    try {
        await fetch('/api/dlq/purge', { method: 'POST' });
        fetchDLQ();
    } catch (e) {
        console.error(e);
    } finally {
        btn.disabled = false;
    }
}

// -------------------------------------------------------------
// Operation Center Handlers
// -------------------------------------------------------------
async function triggerLoad(count) {
    try {
        const response = await fetch('/api/load-test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ count: count })
        });
        const data = await response.json();
        showIngestAlert('success', data.message);
    } catch (error) {
        showIngestAlert('error', 'Failed to generate load: ' + error.message);
    }
}

async function resetMetrics() {
    try {
        const response = await fetch('/api/metrics/reset', { method: 'POST' });
        if (response.ok) {
            lastAckedCount = 0;
            fetchMetrics();
        }
    } catch (e) {
        console.error(e);
    }
}

// -------------------------------------------------------------
// Manual Event Ingestion Gateways
// -------------------------------------------------------------
function updateJobNames() {
    const queue = document.getElementById('ingest-queue').value;
    const jobSelect = document.getElementById('ingest-job-name');
    jobSelect.innerHTML = '';

    let options = [];
    if (queue === 'pdf-generation') {
        options = [['pdf.generate', 'pdf.generate (invoice, reports)']];
    } else if (queue === 'api-sync') {
        options = [['api.sync', 'api.sync (CRM contact update)']];
    } else if (queue === 'data-transform') {
        options = [['data.transform', 'data.transform (Batch scrubber)']];
    }

    options.forEach(opt => {
        const el = document.createElement('option');
        el.value = opt[0];
        el.textContent = opt[1];
        jobSelect.appendChild(el);
    });

    // Populate a sample payload as well
    const payloadArea = document.getElementById('ingest-payload');
    if (queue === 'pdf-generation') {
        payloadArea.value = JSON.stringify({ order_id: Math.floor(10000 + Math.random() * 90000), template: "invoice_pdf", client_email: "hello@nexusflow.io" }, null, 2);
    } else if (queue === 'api-sync') {
        payloadArea.value = JSON.stringify({ endpoint: "crm/contacts", payload: { name: "John Doe", email: "john@example.com" } }, null, 2);
    } else if (queue === 'data-transform') {
        payloadArea.value = JSON.stringify({ records_count: 500, operation: "scrubbing" }, null, 2);
    }
}

function generateUUID() {
    // Generate simple UUIDv4 mock
    const uuid = 'pk-' + 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
    document.getElementById('ingest-idempotency').value = uuid;
}

async function handleManualIngest(e) {
    e.preventDefault();
    const alertEl = document.getElementById('ingest-alert');
    alertEl.className = 'alert hidden';

    const queue = document.getElementById('ingest-queue').value;
    const name = document.getElementById('ingest-job-name').value;
    const idempotency = document.getElementById('ingest-idempotency').value;
    const payloadStr = document.getElementById('ingest-payload').value;

    let payload;
    try {
        payload = json_decode_check(payloadStr);
    } catch (err) {
        showIngestAlert('error', 'Payload is not valid JSON: ' + err.message);
        return;
    }

    try {
        const response = await fetch('/api/events', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                queue: queue,
                name: name,
                idempotency_key: idempotency,
                payload: payload
            })
        });

        const data = await response.json();
        
        if (response.status === 202) {
            showIngestAlert('success', `Event queued successfully! Job ID: ${data.job_id}`);
            generateUUID(); // Refresh key for next ingest
        } else if (response.status === 200) {
            showIngestAlert('success', `Duplicate detected! Event already completed. Status: ${data.status}`);
        } else {
            showIngestAlert('error', data.error || 'Server error occurred');
        }
    } catch (err) {
        showIngestAlert('error', 'Network error: ' + err.message);
    }
}

function json_decode_check(str) {
    return JSON.parse(str);
}

function showIngestAlert(type, message) {
    const alertEl = document.getElementById('ingest-alert');
    alertEl.className = `alert ${type}`;
    alertEl.textContent = message;
    alertEl.classList.remove('hidden');
    
    // Auto-hide success messages after 4 seconds
    if (type === 'success') {
        setTimeout(() => {
            if (alertEl.textContent === message) {
                alertEl.classList.add('hidden');
            }
        }, 4000);
    }
}
