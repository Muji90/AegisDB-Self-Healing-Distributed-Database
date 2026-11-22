let latestCluster = [];
let currentRawResult = null;
let currentEventFilter = "all";
let latestEvents = [];
let latestMetrics = null;
let pendingFailoverNote = null;
let toastTimer = null;


function showToast(message) {
    const toast =
        document.getElementById("toast");

    let duration = 2800;

    // If the last record request was handled by another node because
    // the connected node was down, say so in the same message.
    if (pendingFailoverNote) {
        message = `${message} ${pendingFailoverNote}`;
        pendingFailoverNote = null;
        duration = 5000;
    }

    toast.textContent = message;

    toast.classList.add("show");

    clearTimeout(toastTimer);

    toastTimer = setTimeout(() => {
        toast.classList.remove("show");
    }, duration);
}


function pretty(value) {
    return JSON.stringify(
        value,
        null,
        2
    );
}


/*
 * Failure transparency (client-side failover).
 *
 * Record operations (/api/kv/...) are sent to the node this dashboard is
 * connected to first. If that node is down (no response) or in simulated
 * failure mode, the same request is retried on the next node in the
 * cluster. Every node can coordinate reads and writes, so any live node
 * can serve the request and the user never needs to switch nodes by hand.
 */

function errorMessage(body) {
    return typeof body.detail
        === "string"
            ? body.detail
            : body.detail?.message
                || body.message
                || "The request could not be completed.";
}


async function readBody(response) {
    try {
        return await response.json();
    } catch {
        return {};
    }
}


function nodeIsDown(response, body) {
    return response.status === 503
        && String(body.detail || "")
            .includes("simulated failure");
}


function failoverTargets() {
    const here = window.location.origin;

    const otherNodes = (latestCluster || [])
        .filter(node =>
            node.url
            && node.url !== here
        )
        .map(node => ({
            base: node.url,
            nodeId: node.node_id
        }));

    // "" means the connected node (same origin as this page).
    return [
        { base: "", nodeId: null },
        ...otherNodes
    ];
}


async function request(
    url,
    options = {}
) {
    const useFailover =
        url.startsWith("/api/kv");

    const targets = useFailover
        ? failoverTargets()
        : [{ base: "", nodeId: null }];

    let lastError = null;

    for (const target of targets) {
        let response;

        try {
            response =
                await fetch(
                    target.base + url,
                    options
                );
        } catch (error) {
            // Node did not answer at all (crashed or stopped).
            lastError = new Error(
                "The node could not be reached."
            );

            if (useFailover) {
                continue;
            }

            throw error;
        }

        const body =
            await readBody(response);

        // Node is in simulated failure: try the next node.
        if (
            useFailover
            && nodeIsDown(response, body)
        ) {
            lastError = new Error(
                errorMessage(body)
            );

            continue;
        }

        if (!response.ok) {
            throw new Error(
                errorMessage(body)
            );
        }

        if (target.nodeId) {
            pendingFailoverNote =
                `(The connected node is unavailable, so ${target.nodeId} handled this request.)`;
        }

        return body;
    }

    throw lastError
        || new Error(
            "No node could handle the request."
        );
}


async function loadStatus() {
    try {
        const data =
            await request(
                "/api/status"
            );

        document.getElementById(
            "currentNode"
        ).textContent =
            data.node_id;

        document.getElementById(
            "sidebarNode"
        ).textContent =
            `${data.node_id} • ${data.node_url}`;

        document.getElementById(
            "replicationFactor"
        ).textContent =
            `RF ${data.replication_factor}`;

        document.getElementById(
            "totalRequests"
        ).textContent =
            data.metrics.total_requests;

        document.getElementById(
            "raftNode"
        ).textContent =
            data.node_id;

        document.getElementById(
            "raftState"
        ).textContent =
            data.election.state;

        document.getElementById(
            "raftTerm"
        ).textContent =
            data.election.term;

        document.getElementById(
            "raftLeaderDetail"
        ).textContent =
            data.election.leader_id
            || "Electing";

    } catch (error) {
        console.error(error);
    }
}


async function loadCluster() {
    try {
        const data =
            await request(
                "/api/cluster"
            );

        latestCluster =
            data.nodes;

        const healthy =
            data.nodes.filter(
                node =>
                    node.healthy
                    && !node.failure_mode
            );

        const percentage =
            Math.round(
                (
                    healthy.length
                    / data.nodes.length
                ) * 100
            );

        document.getElementById(
            "healthyNodes"
        ).textContent =
            `${healthy.length}/${data.nodes.length}`;

        document.getElementById(
            "healthPercent"
        ).textContent =
            `${percentage}%`;

        document.getElementById(
            "healthRing"
        ).style.background =
            `conic-gradient(
                var(--accent) ${percentage}%,
                rgba(255,255,255,0.08) 0
            )`;

        const leader =
            data.nodes.find(
                node =>
                    node.election
                    && node.election.state
                    === "LEADER"
                    && node.healthy
                    && !node.failure_mode
            );

        document.getElementById(
            "raftLeader"
        ).textContent =
            leader
                ? leader.node_id
                : "Electing...";

        renderDashboardNodes(
            data.nodes
        );

        renderClusterControls(
            data.nodes
        );

    } catch (error) {
        console.error(error);
    }
}


function renderDashboardNodes(
    nodes
) {
    const container =
        document.getElementById(
            "dashboardNodes"
        );

    container.innerHTML = "";

    nodes.forEach(node => {
        const online =
            node.healthy
            && !node.failure_mode;

        const card =
            document.createElement(
                "div"
            );

        card.className =
            "node-card";

        card.innerHTML = `
            <div class="node-card-top">

                <strong>
                    ${escapeHtml(node.node_id)}
                </strong>

                <small>

                    <span class="
                        status-dot
                        ${
                            online
                            ? "status-online"
                            : "status-offline"
                        }
                    "></span>

                    ${
                        online
                        ? "ONLINE"
                        : "OFFLINE"
                    }

                </small>

            </div>

            <div class="node-meta">

                <span>
                    ${node.records || 0}
                    ${
                        node.records === 1
                        ? "record"
                        : "records"
                    }
                </span>

                <span>
                    ${
                        node.election?.state
                        || "UNKNOWN"
                    }
                </span>

            </div>
        `;

        container.appendChild(
            card
        );
    });
}


function renderClusterControls(
    nodes
) {
    const container =
        document.getElementById(
            "clusterControl"
        );

    container.innerHTML = "";

    nodes.forEach(node => {
        const failed =
            node.failure_mode
            || !node.healthy;

        const card =
            document.createElement(
                "div"
            );

        card.className =
            "control-card";

        card.innerHTML = `
            <div>

                <h4>
                    ${escapeHtml(node.node_id)}
                </h4>

                <p>
                    ${
                        failed
                        ? "Failure simulated"
                        : "Node operational"
                    }
                    •
                    ${
                        node.election?.state
                        || "UNKNOWN"
                    }
                </p>

            </div>

            <button
                class="${
                    failed
                    ? "primary-btn"
                    : "danger-btn"
                }"
                onclick="${
                    failed
                    ? `recoverNode('${node.node_id}')`
                    : `failNode('${node.node_id}')`
                }"
            >
                ${
                    failed
                    ? "Recover Now"
                    : "Simulate Failure"
                }
            </button>
        `;

        container.appendChild(
            card
        );
    });
}


function getFormRecord() {
    const name =
        document.getElementById(
            "recordName"
        ).value.trim();

    const program =
        document.getElementById(
            "recordProgram"
        ).value.trim();

    const yearRaw =
        document.getElementById(
            "recordYear"
        ).value.trim();

    if (!name) {
        throw new Error(
            "Please enter a name."
        );
    }

    if (!program) {
        throw new Error(
            "Please enter a program."
        );
    }

    if (!yearRaw) {
        throw new Error(
            "Please enter a year."
        );
    }

    const year =
        Number(yearRaw);

    if (
        !Number.isInteger(year)
        || year < 1
    ) {
        throw new Error(
            "Year must be a valid whole number."
        );
    }

    return {
        name,
        program,
        year
    };
}


async function putRecord() {
    const key =
        document.getElementById(
            "putKey"
        ).value.trim();

    const message =
        document.getElementById(
            "writeSuccess"
        );

    message.classList.add(
        "hidden"
    );

    if (!key) {
        showToast(
            "Please enter a record key."
        );

        return;
    }

    let value;

    try {
        value =
            getFormRecord();

    } catch (error) {
        showToast(
            error.message
        );

        return;
    }

    try {
        const data =
            await request(
                `/api/kv/${encodeURIComponent(key)}`,
                {
                    method: "POST",

                    headers: {
                        "Content-Type":
                            "application/json"
                    },

                    body:
                        JSON.stringify({
                            value
                        })
                }
            );

        message.innerHTML = `
            <strong>
                Record replicated successfully.
            </strong>

            <br>

            Stored on:
            ${data.replicated_to
                .map(escapeHtml)
                .join(", ")}

            <br>

            Write acknowledgements:
            ${data.acknowledgements}
            /
            ${data.write_quorum}
            required
        `;

        message.classList.remove(
            "hidden"
        );

        showToast(
            "Record successfully replicated."
        );

        document.getElementById(
            "getKey"
        ).value =
            key;

        await refreshAll();

    } catch (error) {
        showToast(
            error.message
        );
    }
}


function formatVectorClock(
    clock
) {
    if (
        !clock
        || Object.keys(clock).length
        === 0
    ) {
        return "None";
    }

    return Object.entries(
        clock
    )
    .map(
        ([node, value]) =>
            `${node}: ${value}`
    )
    .join("  •  ");
}


function showRecord(
    data
) {
    currentRawResult =
        data;

    const value =
        data.value || {};

    document.getElementById(
        "emptyRecordState"
    ).classList.add(
        "hidden"
    );

    document.getElementById(
        "recordError"
    ).classList.add(
        "hidden"
    );

    document.getElementById(
        "recordCard"
    ).classList.remove(
        "hidden"
    );

    document.getElementById(
        "resultKey"
    ).textContent =
        data.key || "-";

    document.getElementById(
        "resultName"
    ).textContent =
        value.name ?? "-";

    document.getElementById(
        "resultProgram"
    ).textContent =
        value.program ?? "-";

    document.getElementById(
        "resultYear"
    ).textContent =
        value.year ?? "-";

    document.getElementById(
        "resultVectorClock"
    ).textContent =
        formatVectorClock(
            data.vector_clock
        );

    document.getElementById(
        "resultReplicas"
    ).textContent =
        `${data.replica_responses}`;

    document.getElementById(
        "resultQuorum"
    ).textContent =
        `${data.replica_responses} responses / ${data.read_quorum} required ✓`;

    document.getElementById(
        "resultConflicts"
    ).textContent =
        data.conflict_versions === 0
        ? "None ✓"
        : `${data.conflict_versions} detected`;

    document.getElementById(
        "rawJson"
    ).textContent =
        pretty(data);

    document.getElementById(
        "rawJson"
    ).classList.add(
        "hidden"
    );

    document.getElementById(
        "rawToggle"
    ).textContent =
        "View Raw JSON";
}


function showRecordError(
    message
) {
    document.getElementById(
        "emptyRecordState"
    ).classList.add(
        "hidden"
    );

    document.getElementById(
        "recordCard"
    ).classList.add(
        "hidden"
    );

    const errorBox =
        document.getElementById(
            "recordError"
        );

    errorBox.textContent =
        message;

    errorBox.classList.remove(
        "hidden"
    );
}


async function getRecord() {
    const key =
        document.getElementById(
            "getKey"
        ).value.trim();

    if (!key) {
        showToast(
            "Please enter a record key."
        );

        return;
    }

    try {
        const data =
            await request(
                `/api/kv/${encodeURIComponent(key)}`
            );

        showRecord(
            data
        );

        showToast(
            "Record retrieved successfully."
        );

    } catch (error) {
        showRecordError(
            error.message
        );

        showToast(
            "Record could not be retrieved."
        );
    }
}


async function deleteRecord() {
    const key =
        document.getElementById(
            "getKey"
        ).value.trim();

    if (!key) {
        showToast(
            "Please enter a record key."
        );

        return;
    }

    const confirmed =
        window.confirm(
            `Delete ${key} from the distributed database?`
        );

    if (!confirmed) {
        return;
    }

    try {
        await request(
            `/api/kv/${encodeURIComponent(key)}`,
            {
                method: "DELETE"
            }
        );

        currentRawResult =
            null;

        document.getElementById(
            "recordCard"
        ).classList.add(
            "hidden"
        );

        document.getElementById(
            "recordError"
        ).classList.add(
            "hidden"
        );

        document.getElementById(
            "emptyRecordState"
        ).classList.remove(
            "hidden"
        );

        showToast(
            "Distributed delete committed."
        );

        await refreshAll();

    } catch (error) {
        showRecordError(
            error.message
        );

        showToast(
            "Delete failed."
        );
    }
}


function toggleRawJson() {
    if (!currentRawResult) {
        return;
    }

    const raw =
        document.getElementById(
            "rawJson"
        );

    const button =
        document.getElementById(
            "rawToggle"
        );

    const isHidden =
        raw.classList.contains(
            "hidden"
        );

    if (isHidden) {
        raw.classList.remove(
            "hidden"
        );

        button.textContent =
            "Hide Raw JSON";

    } else {
        raw.classList.add(
            "hidden"
        );

        button.textContent =
            "View Raw JSON";
    }
}


async function loadKeys() {
    try {
        const data =
            await request(
                "/api/keys"
            );

        const table =
            document.getElementById(
                "recordsTable"
            );

        table.innerHTML = "";

        if (
            data.records.length
            === 0
        ) {
            table.innerHTML = `
                <tr>
                    <td colspan="3">
                        No records are currently
                        stored on this node.
                    </td>
                </tr>
            `;

            return;
        }

        data.records.forEach(
            record => {

                const row =
                    document.createElement(
                        "tr"
                    );

                const value =
                    record.value || {};

                const name =
                    value.name
                    ?? "Unnamed Record";

                const program =
                    value.program
                    ?? "";

                row.innerHTML = `
                    <td>
                        <strong>
                            ${escapeHtml(record.key)}
                        </strong>
                    </td>

                    <td>
                        <div class="table-record-value">

                            <strong>
                                ${escapeHtml(String(name))}
                            </strong>

                            <span>
                                ${escapeHtml(String(program))}
                            </span>

                        </div>
                    </td>

                    <td>
                        ${escapeHtml(
                            formatVectorClock(
                                record.vector_clock
                            )
                        )}
                    </td>
                `;

                table.appendChild(
                    row
                );
            }
        );

    } catch (error) {
        console.error(error);
    }
}


function formatEventTime(
    timestamp
) {
    const date =
        new Date(
            timestamp * 1000
        );

    return date.toLocaleTimeString(
        [],
        {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit"
        }
    );
}


function renderEvents() {
    const list =
        document.getElementById(
            "eventList"
        );

    if (!list) {
        return;
    }

    const events =
        currentEventFilter === "all"
        ? latestEvents
        : latestEvents.filter(
            event =>
                event.category
                === currentEventFilter
        );

    if (
        events.length === 0
    ) {
        list.innerHTML = `
            <div class="event-empty">

                <div class="event-empty-icon">
                    ≋
                </div>

                <strong>
                    No events in this category
                </strong>

                <p>
                    New distributed system activity
                    will appear here automatically.
                </p>

            </div>
        `;

        return;
    }

    list.innerHTML = "";

    events.forEach(
        event => {

            const item =
                document.createElement(
                    "div"
                );

            item.className =
                "event-entry";

            const severity =
                [
                    "info",
                    "success",
                    "warning",
                    "danger"
                ].includes(
                    event.severity
                )
                ? event.severity
                : "info";

            item.innerHTML = `
                <span
                    class="
                        event-status-dot
                        event-status-${severity}
                    "
                ></span>

                <span class="event-time">
                    ${escapeHtml(
                        formatEventTime(
                            event.timestamp
                        )
                    )}
                </span>

                <span class="event-category">
                    ${escapeHtml(
                        event.category
                        || "system"
                    )}
                </span>

                <span class="event-message">
                    ${escapeHtml(
                        event.message
                    )}
                </span>

                <span
                    class="event-node"
                    title="Reporter node"
                >
                    Reporter: ${escapeHtml(
                        event.node_id
                        || "-"
                    )}
                </span>
            `;

            list.appendChild(
                item
            );
        }
    );
}


async function loadEvents() {
    try {
        const data =
            await request(
                "/api/events"
            );

        latestEvents =
            data.events || [];

        const counts =
            data.counts || {};

        document.getElementById(
            "eventFailureCount"
        ).textContent =
            counts.failures || 0;

        document.getElementById(
            "eventElectionCount"
        ).textContent =
            counts.leader_elections || 0;

        document.getElementById(
            "eventHealCount"
        ).textContent =
            counts.self_heals || 0;

        document.getElementById(
            "eventReadRepairCount"
        ).textContent =
            counts.read_repairs || 0;

        document.getElementById(
            "eventRebalanceCount"
        ).textContent =
            counts.rebalances || 0;

        renderEvents();

    } catch (error) {
        console.error(error);
    }
}


function prepareCanvas(canvas) {
    if (!canvas) {
        return null;
    }

    const ratio = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(320, Math.floor(rect.width));
    const height = 285;

    canvas.width = Math.floor(width * ratio);
    canvas.height = Math.floor(height * ratio);

    const context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);

    return {
        context,
        width,
        height
    };
}


function drawGrid(context, width, height, rows = 4) {
    context.clearRect(0, 0, width, height);
    context.strokeStyle = "rgba(255,255,255,0.07)";
    context.lineWidth = 1;

    for (let index = 0; index <= rows; index += 1) {
        const y = 28 + ((height - 58) / rows) * index;
        context.beginPath();
        context.moveTo(42, y);
        context.lineTo(width - 16, y);
        context.stroke();
    }
}


function drawLineSeries(canvasId, series, options = {}) {
    const canvas = document.getElementById(canvasId);
    const prepared = prepareCanvas(canvas);

    if (!prepared) {
        return;
    }

    const {context, width, height} = prepared;
    drawGrid(context, width, height);

    const allValues = series.flatMap(item => item.values || []);
    const maxValue = Math.max(options.minimumMax || 1, ...allValues, 1);
    const left = 42;
    const right = width - 16;
    const top = 28;
    const bottom = height - 30;
    const plotWidth = Math.max(1, right - left);
    const plotHeight = Math.max(1, bottom - top);

    context.fillStyle = "rgba(140,160,179,0.85)";
    context.font = "10px Segoe UI";
    context.textAlign = "right";

    for (let index = 0; index <= 4; index += 1) {
        const value = Math.round(maxValue - (maxValue / 4) * index);
        const y = top + (plotHeight / 4) * index;
        context.fillText(String(value), left - 8, y + 3);
    }

    const palette = [
        "#58d7b9",
        "#77a8ff",
        "#ffc96b",
        "#ff8aa0",
        "#b69cff"
    ];

    series.forEach((item, seriesIndex) => {
        const values = item.values || [];

        if (values.length === 0) {
            return;
        }

        context.strokeStyle = palette[seriesIndex % palette.length];
        context.fillStyle = palette[seriesIndex % palette.length];
        context.lineWidth = 2;
        context.beginPath();

        values.forEach((value, index) => {
            const x = values.length === 1
                ? left
                : left + (index / (values.length - 1)) * plotWidth;
            const y = bottom - (value / maxValue) * plotHeight;

            if (index === 0) {
                context.moveTo(x, y);
            } else {
                context.lineTo(x, y);
            }
        });

        context.stroke();

        values.forEach((value, index) => {
            const x = values.length === 1
                ? left
                : left + (index / (values.length - 1)) * plotWidth;
            const y = bottom - (value / maxValue) * plotHeight;

            context.beginPath();
            context.arc(x, y, 2.5, 0, Math.PI * 2);
            context.fill();
        });
    });

    context.textAlign = "left";
    context.font = "10px Segoe UI";

    series.forEach((item, index) => {
        context.fillStyle = palette[index % palette.length];
        context.fillRect(left + index * 105, height - 13, 8, 8);
        context.fillStyle = "rgba(140,160,179,0.95)";
        context.fillText(item.label, left + 12 + index * 105, height - 6);
    });
}


function drawHealthChart(samples) {
    const nodeIds = [
        "node1",
        "node2",
        "node3",
        "node4",
        "node5"
    ];

    const canvas =
        document.getElementById(
            "healthChart"
        );

    const prepared =
        prepareCanvas(
            canvas
        );

    if (!prepared) {
        return;
    }

    const {
        context,
        width,
        height
    } = prepared;

    context.clearRect(
        0,
        0,
        width,
        height
    );

    const left = 60;
    const right = width - 18;
    const top = 22;
    const rowHeight = 39;

    const onlineColor =
        "#16856F";

    const offlineColor =
        "#991F35";

    const textColor =
        "rgba(140,160,179,0.95)";

    const gridColor =
        "rgba(255,255,255,0.06)";

    const count =
        Math.max(
            samples.length,
            1
        );

    const segmentWidth =
        (right - left)
        / count;

    function getSampleTime(
        sample,
        index
    ) {
        if (
            sample
            && sample.timestamp
            !== undefined
            && sample.timestamp
            !== null
        ) {
            const raw =
                sample.timestamp;

            if (
                typeof raw
                === "number"
            ) {
                return new Date(
                    raw > 100000000000
                        ? raw
                        : raw * 1000
                );
            }

            const parsed =
                new Date(
                    raw
                );

            if (
                !Number.isNaN(
                    parsed.getTime()
                )
            ) {
                return parsed;
            }
        }

        if (
            sample
            && sample.time
            !== undefined
            && sample.time
            !== null
        ) {
            const raw =
                sample.time;

            if (
                typeof raw
                === "number"
            ) {
                return new Date(
                    raw > 100000000000
                        ? raw
                        : raw * 1000
                );
            }

            const parsed =
                new Date(
                    raw
                );

            if (
                !Number.isNaN(
                    parsed.getTime()
                )
            ) {
                return parsed;
            }
        }

        const secondsAgo =
            (
                samples.length
                - 1
                - index
            ) * 2;

        return new Date(
            Date.now()
            - secondsAgo * 1000
        );
    }

    function formatTime(
        date
    ) {
        return date.toLocaleTimeString(
            [],
            {
                hour:
                    "2-digit",

                minute:
                    "2-digit",

                second:
                    "2-digit"
            }
        );
    }

    nodeIds.forEach(
        (
            nodeId,
            nodeIndex
        ) => {

            const y =
                top
                + nodeIndex
                * rowHeight;

            context.fillStyle =
                textColor;

            context.font =
                "10px Segoe UI";

            context.textAlign =
                "right";

            context.fillText(
                nodeId,
                left - 10,
                y + 13
            );

            context.strokeStyle =
                gridColor;

            context.beginPath();

            context.moveTo(
                left,
                y + 10
            );

            context.lineTo(
                right,
                y + 10
            );

            context.stroke();

            samples.forEach(
                (
                    sample,
                    index
                ) => {

                    const healthy =
                        Boolean(
                            sample
                                .healthy_nodes
                                ?.[nodeId]
                        );

                    context.fillStyle =
                        healthy
                            ? onlineColor
                            : offlineColor;

                    context.fillRect(
                        left
                        + index
                        * segmentWidth,

                        y,

                        Math.max(
                            2,
                            segmentWidth
                            + 0.4
                        ),

                        20
                    );
                }
            );
        }
    );

    const timelineY =
        top
        + nodeIds.length
        * rowHeight
        + 5;

    context.strokeStyle =
        "rgba(255,255,255,0.10)";

    context.beginPath();

    context.moveTo(
        left,
        timelineY
    );

    context.lineTo(
        right,
        timelineY
    );

    context.stroke();

    if (
        samples.length
        > 0
    ) {
        const tickCount =
            Math.min(
                5,
                samples.length
            );

        context.font =
            "9px Segoe UI";

        context.fillStyle =
            "rgba(140,160,179,0.85)";

        for (
            let tick = 0;
            tick < tickCount;
            tick += 1
        ) {
            const ratio =
                tickCount === 1
                    ? 0
                    : tick
                    / (
                        tickCount
                        - 1
                    );

            const sampleIndex =
                Math.round(
                    ratio
                    * (
                        samples.length
                        - 1
                    )
                );

            const x =
                left
                + ratio
                * (
                    right
                    - left
                );

            const sampleTime =
                getSampleTime(
                    samples[
                        sampleIndex
                    ],
                    sampleIndex
                );

            const label =
                formatTime(
                    sampleTime
                );

            context.strokeStyle =
                "rgba(255,255,255,0.13)";

            context.beginPath();

            context.moveTo(
                x,
                timelineY
            );

            context.lineTo(
                x,
                timelineY
                + 5
            );

            context.stroke();

            context.fillStyle =
                "rgba(140,160,179,0.85)";

            if (
                tick === 0
            ) {
                context.textAlign =
                    "left";

            } else if (
                tick
                === tickCount - 1
            ) {
                context.textAlign =
                    "right";

            } else {
                context.textAlign =
                    "center";
            }

            context.fillText(
                label,
                x,
                timelineY + 17
            );
        }
    }

    context.textAlign =
        "left";

    context.font =
        "9px Segoe UI";

    const legendY =
        height - 13;

    context.fillStyle =
        onlineColor;

    context.fillRect(
        left,
        legendY - 7,
        8,
        8
    );

    context.fillStyle =
        textColor;

    context.fillText(
        "Online",
        left + 13,
        legendY
    );

    context.fillStyle =
        offlineColor;

    context.fillRect(
        left + 65,
        legendY - 7,
        8,
        8
    );

    context.fillStyle =
        textColor;

    context.fillText(
        "Offline",
        left + 78,
        legendY
    );

    if (
        samples.length
        > 1
    ) {
        const firstTime =
            getSampleTime(
                samples[0],
                0
            );

        const lastTime =
            getSampleTime(
                samples[
                    samples.length
                    - 1
                ],
                samples.length
                - 1
            );

        const durationSeconds =
            Math.max(
                0,
                Math.round(
                    (
                        lastTime.getTime()
                        - firstTime.getTime()
                    )
                    / 1000
                )
            );

        context.textAlign =
            "right";

        context.fillStyle =
            "rgba(140,160,179,0.70)";

        context.fillText(
            `Live window: ${durationSeconds}s`,
            right,
            legendY
        );
    }
}

function drawLeaderChart(samples) {
    const leaderNumber = leader => {
        if (!leader) {
            return 0;
        }
        const match = String(leader).match(/(\d+)/);
        return match ? Number(match[1]) : 0;
    };

    drawLineSeries(
        "raftChart",
        [
            {
                label: "Leader node",
                values: samples.map(sample => leaderNumber(sample.leader))
            },
            {
                label: "Election term",
                values: samples.map(sample => Number(sample.term || 0))
            }
        ],
        {minimumMax: 5}
    );
}


function renderMetricObservations(samples, summary) {
    const throughput = document.getElementById("metricThroughputObservation");
    const health = document.getElementById("metricHealthObservation");
    const consensus = document.getElementById("metricConsensusObservation");
    const recovery = document.getElementById("metricRecoveryObservation");

    if (!throughput || !health || !consensus || !recovery) {
        return;
    }

    let recentThroughput = 0;
    if (samples.length >= 2) {
        const last = samples[samples.length - 1];
        const previous = samples[samples.length - 2];
        recentThroughput = Math.max(
            0,
            Number(last.total_requests || 0) - Number(previous.total_requests || 0)
        );
    }

    throughput.textContent = recentThroughput > 0
        ? `${recentThroughput} request(s) were processed in the latest two-second sample.`
        : "The cluster is currently idle; new GET and PUT operations will appear on the throughput chart.";

    health.textContent = `${summary.healthy_nodes}/${summary.total_nodes} nodes are healthy. The timeline records each node's online/offline state during failure tests.`;

    consensus.textContent = `${summary.leader} is the current leader (Bully election term ${summary.term}). Leadership changes are plotted as elections occur.`;

    const recoveryTotal = Number(summary.self_heals || 0)
        + Number(summary.read_repairs || 0)
        + Number(summary.rebalances || 0);

    recovery.textContent = recoveryTotal > 0
        ? `${summary.self_heals} self-heal(s), ${summary.read_repairs} read repair(s), and ${summary.rebalances} rebalance(s) have been recorded.`
        : "No recovery operation has been required since this cluster session started.";
}


function renderMetrics(data) {
    latestMetrics = data;

    const samples = data.samples || [];
    const summary = data.summary || {};

    document.getElementById("metricReadLatency").textContent =
        `${Number(summary.average_read_latency_ms || 0).toFixed(2)} ms`;

    document.getElementById("metricWriteLatency").textContent =
        `${Number(summary.average_write_latency_ms || 0).toFixed(2)} ms`;

    document.getElementById("metricQuorumSuccess").textContent =
        `${Number(summary.quorum_success_rate ?? 100).toFixed(1)}%`;

    document.getElementById("metricLeader").textContent =
        summary.leader || "Electing";

    document.getElementById("metricTerm").textContent =
        `Election term ${summary.term || 0}`;

    const throughputValues = samples.map((sample, index) => {
        if (index === 0) {
            return 0;
        }

        return Math.max(
            0,
            Number(sample.total_requests || 0)
            - Number(samples[index - 1].total_requests || 0)
        );
    });

    drawLineSeries(
        "throughputChart",
        [
            {
                label: "Requests",
                values: throughputValues
            }
        ],
        {minimumMax: 5}
    );

    drawHealthChart(samples);
    drawLeaderChart(samples);

    drawLineSeries(
        "recoveryChart",
        [
            {
                label: "Self-heals",
                values: samples.map(sample => Number(sample.self_heals || 0))
            },
            {
                label: "Read repairs",
                values: samples.map(sample => Number(sample.read_repairs || 0))
            },
            {
                label: "Rebalances",
                values: samples.map(sample => Number(sample.rebalances || 0))
            }
        ],
        {minimumMax: 3}
    );

    renderMetricObservations(samples, summary);
}


async function loadMetrics() {
    try {
        const data = await request("/api/metrics");
        renderMetrics(data);
    } catch (error) {
        console.error(error);
    }
}


function escapeHtml(text) {
    const element =
        document.createElement(
            "div"
        );

    element.textContent =
        String(text);

    return element.innerHTML;
}


async function failNode(
    nodeId
) {
    try {
        const data = await request(
            `/api/admin/fail/${nodeId}`,
            {
                method: "POST"
            }
        );

        const delay = Number(
            data.auto_recovery_in_seconds || 0
        );

        showToast(
            delay > 0
                ? `${nodeId} failed. Automatic recovery expected in about ${delay.toFixed(1)} seconds.`
                : `${nodeId} failure simulated.`
        );

        setTimeout(
            refreshAll,
            1200
        );

    } catch (error) {
        showToast(
            error.message
        );
    }
}


async function recoverNode(
    nodeId
) {
    try {
        await request(
            `/api/admin/recover/${nodeId}`,
            {
                method: "POST"
            }
        );

        showToast(
            `${nodeId} recovered.`
        );

        setTimeout(
            refreshAll,
            1000
        );

    } catch (error) {
        showToast(
            error.message
        );
    }
}


async function refreshAll() {
    await Promise.all([
        loadStatus(),
        loadCluster(),
        loadKeys(),
        loadEvents(),
        loadMetrics()
    ]);
}


document.querySelectorAll(
    ".nav-item"
).forEach(
    button => {

        button.addEventListener(
            "click",
            () => {

                document.querySelectorAll(
                    ".nav-item"
                ).forEach(
                    item =>
                        item.classList.remove(
                            "active"
                        )
                );

                button.classList.add(
                    "active"
                );

                const section =
                    button.dataset.section;

                document.querySelectorAll(
                    ".page-section"
                ).forEach(
                    page =>
                        page.classList.remove(
                            "active"
                        )
                );

                document.getElementById(
                    section
                ).classList.add(
                    "active"
                );

                const titles = {
                    dashboard:
                        "Cluster Overview",

                    database:
                        "Distributed Data Explorer",

                    cluster:
                        "Failure Management",

                    events:
                        "Live Observability",

                    metrics:
                        "Performance Metrics",

                    about:
                        "System Architecture"
                };

                document.getElementById(
                    "pageTitle"
                ).textContent =
                    titles[section];
            }
        );
    }
);


document.querySelectorAll(
    ".event-filter"
).forEach(
    button => {

        button.addEventListener(
            "click",
            () => {

                document.querySelectorAll(
                    ".event-filter"
                ).forEach(
                    item =>
                        item.classList.remove(
                            "active"
                        )
                );

                button.classList.add(
                    "active"
                );

                currentEventFilter =
                    button.dataset
                    .eventFilter;

                renderEvents();
            }
        );
    }
);


refreshAll();


setInterval(
    () => {
        loadStatus();
        loadCluster();
        loadEvents();
        loadMetrics();
    },
    2000
);