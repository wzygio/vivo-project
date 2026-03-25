// ==========================================================
//  1. 侧边栏渲染逻辑 (支持二级折叠)
// ==========================================================
function renderSidebar() {
    const container = document.getElementById('sidebar-nav-container');
    if (!container || !APP_CONFIG || !APP_CONFIG.sidebarLinks) return;

    container.innerHTML = ''; // 清空现有内容

    APP_CONFIG.sidebarLinks.forEach(item => {
        if (item.children && item.children.length > 0) {
            // === 父级菜单 ===
            const groupDiv = document.createElement('div');
            groupDiv.className = 'nav-group';

            // Header
            const header = document.createElement('div');
            header.className = 'nav-group-header';
            header.innerHTML = `
                <span>${item.name}</span>
                <svg class="nav-arrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M9 18l6-6-6-6" stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
            `;

            // Children
            const childrenDiv = document.createElement('div');
            childrenDiv.className = 'nav-children';

            item.children.forEach(sub => {
                const subLink = document.createElement('a');
                subLink.href = sub.url;
                subLink.className = 'nav-child-item';
                subLink.textContent = sub.name;
                if (sub.url && sub.url !== "#") subLink.target = "_blank";
                childrenDiv.appendChild(subLink);
            });

            // Toggle Event
            header.onclick = () => {
                const isExpanded = header.classList.contains('expanded');
                if (isExpanded) {
                    header.classList.remove('expanded');
                    childrenDiv.classList.remove('show');
                } else {
                    header.classList.add('expanded');
                    childrenDiv.classList.add('show');
                }
            };

            groupDiv.appendChild(header);
            groupDiv.appendChild(childrenDiv);
            container.appendChild(groupDiv);

        } else {
            // === 普通链接 ===
            const link = document.createElement('a');
            link.href = item.url;
            link.className = 'nav-item';
            link.textContent = item.name;
            if (item.url && item.url !== "#") link.target = "_blank";
            container.appendChild(link);
        }
    });
}

// 立即渲染侧边栏
renderSidebar();

// ==========================================================
//  2. 侧边栏控制
// ==========================================================
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebar-overlay');
    
    if (sidebar.classList.contains('open')) {
        sidebar.classList.remove('open');
        overlay.classList.remove('show');
    } else {
        sidebar.classList.add('open');
        overlay.classList.add('show');
    }
}

// ==========================================================
//  3. 技能树逻辑
// ==========================================================
const DATA = APP_CONFIG.treeData || [];

const svgLayer = document.getElementById('svg-lines');
const nodesLayer = document.getElementById('nodes-layer');
const coreLayer = document.getElementById('core-layer');
const sceneContainer = document.getElementById('scene-container');
let activeId = null;

function getEdgePoint(cx, cy, tx, ty, radius) {
    const angle = Math.atan2(ty - cy, tx - cx);
    return { x: cx + Math.cos(angle) * radius, y: cy + Math.sin(angle) * radius };
}

function render() {
    svgLayer.innerHTML = ''; nodesLayer.innerHTML = ''; coreLayer.innerHTML = '';
    const w = window.innerWidth; const h = window.innerHeight;
    const cx = w / 2; const cy = h / 2;
    const radius = Math.min(w, h) * 0.32; 
    const R_CORE = 60; const R_MAIN = 40; const R_SUB = 35;

    // 核心节点
    const coreNode = document.createElement('div');
    coreNode.className = 'node-container node-center pointer-auto';
    coreNode.style.left = `${cx}px`; coreNode.style.top = `${cy}px`;

    // =========== 【还原为您指定的柔和星星】 ===========
    coreNode.innerHTML = `
        <div class="node-bubble core-star-bubble">
            <div class="breathing-layer"></div>
            
            <svg class="w-24 h-24 star-icon" viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M16,2 L19.8,11.3 L29.5,12.1 L22.1,18.5 L24.4,28 L16,22.8 L7.6,28 L9.9,18.5 L2.5,12.1 L12.2,11.3 L16,2 Z" transform="translate(1, 1) scale(0.94)"/>
            </svg>
        </div>
        <div class="node-label" style="margin-top: 15px; font-size: 15px; font-weight: 600;">V3北极星自动预警</div>
    `;
    // ===============================================

    // [修改点]：为核心节点添加读取配置并跳转的逻辑
    coreNode.onclick = (e) => { 
        e.stopPropagation(); 
        resetCamera(); // 保持原有的视角重置动画
        
        // 读取配置中的核心节点链接并执行跳转
        if (APP_CONFIG.coreUrl && APP_CONFIG.coreUrl !== "#") {
            window.open(APP_CONFIG.coreUrl, '_blank'); 
            // 注意：如果你希望在当前页面直接跳转而不是打开新标签页，可以将 '_blank' 改为 '_self'
        }
    };
    coreLayer.appendChild(coreNode);

    // 主节点
    DATA.forEach((data, index) => {
        const angle = (index * (360 / DATA.length) - 90) * (Math.PI / 180);
        const x = cx + radius * Math.cos(angle);
        const y = cy + radius * Math.sin(angle);

        const start = getEdgePoint(cx, cy, x, y, R_CORE + 5);
        const end = getEdgePoint(x, y, cx, cy, R_MAIN + 5);
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", start.x); line.setAttribute("y1", start.y);
        line.setAttribute("x2", end.x); line.setAttribute("y2", end.y);
        line.setAttribute("class", "link-dashed");
        svgLayer.appendChild(line);

        const node = document.createElement('div');
        const isActive = (activeId === index);
        node.className = `node-container pointer-auto ${isActive ? 'active' : ''}`;
        node.style.left = `${x}px`; node.style.top = `${y}px`;
        node.innerHTML = `<div class="node-bubble"><svg width="28" height="28" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${data.icon}</svg></div><div class="node-label">${data.label}</div>`;
        
        node.onclick = (e) => { 
            e.stopPropagation(); 
            if (isActive) resetCamera();
            else focusNode(index, x, y);
        };
        nodesLayer.appendChild(node);

        if (isActive) {
            data.subs.forEach((sub, i) => {
                const subAngle = angle + (i - 1) * 0.6;
                const dist = 200;
                const sx = x + dist * Math.cos(subAngle);
                const sy = y + dist * Math.sin(subAngle);

                const subStart = getEdgePoint(x, y, sx, sy, R_MAIN);
                const subEnd = getEdgePoint(sx, sy, x, y, R_SUB);
                const activeLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
                activeLine.setAttribute("x1", subStart.x); activeLine.setAttribute("y1", subStart.y);
                activeLine.setAttribute("x2", subEnd.x); activeLine.setAttribute("y2", subEnd.y);
                activeLine.setAttribute("class", "link-solid");
                setTimeout(() => svgLayer.appendChild(activeLine), i * 50);

                const subEl = document.createElement('div');
                subEl.className = 'sub-node-container pointer-auto';
                subEl.style.left = `${sx}px`; subEl.style.top = `${sy}px`;
                subEl.style.cursor = sub.url && sub.url !== "#" ? 'pointer' : 'default';

                subEl.innerHTML = `<div class="sub-bubble"><div class="sub-label-text">${sub.l}</div><div class="sub-value-text">${sub.v}</div></div>`;
                
                subEl.onclick = (e) => {
                    e.stopPropagation();
                    if (sub.url && sub.url !== "#") {
                        window.open(sub.url, '_blank'); 
                    }
                };

                nodesLayer.appendChild(subEl);
                requestAnimationFrame(() => subEl.classList.add('visible'));
            });
        }
    });
}

function focusNode(index, targetX, targetY) {
    activeId = index;
    const w = window.innerWidth; const h = window.innerHeight;
    const offsetX = (w / 2) - targetX; const offsetY = (h / 2) - targetY;
    sceneContainer.style.transform = `translate(${offsetX}px, ${offsetY}px) scale(1.1)`;
    render();
}
function resetCamera() {
    activeId = null; sceneContainer.style.transform = `translate(0px, 0px) scale(1)`; render();
}
window.addEventListener('resize', () => { resetCamera(); });
render();

// ==========================================================
//  4. 粒子动画
// ==========================================================
const canvas = document.getElementById('canvas-interactive');
const ctx = canvas.getContext('2d');
canvas.width = window.innerWidth; canvas.height = window.innerHeight;
let particles = [];
let mouse = { x: null, y: null, radius: 0 };

window.addEventListener('resize', () => {
    canvas.width = window.innerWidth; canvas.height = window.innerHeight;
    mouse.radius = ((canvas.height/80) * (canvas.width/80));
    initParticles();
});
window.addEventListener('mouseout', () => { mouse.x = undefined; mouse.y = undefined });

class Particle {
    constructor(x, y, dx, dy, size, color) {
        this.x = x; this.y = y; this.dx = dx; this.dy = dy; this.size = size; this.color = color;
    }
    draw() { ctx.beginPath(); ctx.arc(this.x, this.y, this.size, 0, Math.PI*2); ctx.fillStyle = this.color; ctx.fill(); }
    update() {
        if (this.x > canvas.width || this.x < 0) this.dx = -this.dx;
        if (this.y > canvas.height || this.y < 0) this.dy = -this.dy;
        let dx = mouse.x - this.x; let dy = mouse.y - this.y;
        let dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < mouse.radius + this.size) {
            if (mouse.x < this.x && this.x < canvas.width - this.size * 10) this.x += 10;
            if (mouse.x > this.x && this.x > this.size * 10) this.x -= 10;
            if (mouse.y < this.y && this.y < canvas.height - this.size * 10) this.y += 10;
            if (mouse.y > this.y && this.y > this.size * 10) this.y -= 10;
        }
        this.x += this.dx; this.y += this.dy; this.draw();
    }
}
function initParticles() {
    particles = [];
    let num = (canvas.height * canvas.width) / 10000;
    for (let i = 0; i < num; i++) {
        let size = (Math.random() * 3) + 1;
        let x = (Math.random() * ((innerWidth - size * 2) - (size * 2)) + size * 2);
        let y = (Math.random() * ((innerHeight - size * 2) - (size * 2)) + size * 2);
        let dx = (Math.random() * 2) - 1; let dy = (Math.random() * 2) - 1;
        particles.push(new Particle(x, y, dx, dy, size, '#0ea5e9'));
    }
}
function connectParticles() {
    for (let a = 0; a < particles.length; a++) {
        for (let b = a; b < particles.length; b++) {
            let dist = ((particles[a].x - particles[b].x)**2) + ((particles[a].y - particles[b].y)**2);
            if (dist < (canvas.width/7) * (canvas.height/7)) {
                ctx.strokeStyle = 'rgba(14, 165, 233,' + (1 - dist/20000)*0.4 + ')';
                ctx.lineWidth = 1;
                ctx.beginPath(); ctx.moveTo(particles[a].x, particles[a].y); ctx.lineTo(particles[b].x, particles[b].y); ctx.stroke();
            }
        }
    }
}
function animate() {
    requestAnimationFrame(animate); ctx.clearRect(0, 0, innerWidth, innerHeight);
    for (let i = 0; i < particles.length; i++) { particles[i].update(); }
    connectParticles();
}
initParticles(); animate();