/* ==========================================================================
 * 全局链接资源池 (Source of Truth)
 * 以后修改链接，只需要改这里，侧边栏和技能树会自动同步更新！
 * ========================================================================== */
const LINKS = {
    // 1. 本地 Streamlit 系统路由
    PORTAL_HOME: "/", 
    ANALYSIS_FILES: "/ANALYSIS_FILES",

    // 2. Streamlit
    YIELD_BY_LOT: "http://10.72.26.31:8503/入库不良率ByLot明细表",
    YIELD_BY_SHEET: "http://10.72.26.31:8503/入库不良率BySheet明细表",
    YIELD_TREND_MWD: "http://10.72.26.31:8503/入库不良率分析看板",
    ANALYSIS_FILES: "http://10.72.26.31:8503/专项资料-解析报告",
    PROJECT_FILES: "http://10.72.26.31:8503/专项资料-台账周报",
    WARNING_DASHBOARD: "http://10.72.26.31:8503/自动预警看板",

    // 3. FineReport
    SPC: "http://10.73.17.76:8080/webroot/decision/v10/entry/access/588faa78-b65f-4725-9ab3-a93b11896c4b?preview=true",
    AOI: "http://10.73.17.17:8080/webroot/decision/view/report?viewlet=Yield_Monitoring%252FDefect_Monitoring%252FDB107.SPC_AOI_TDSUM_Analyze_for_BJX_Report.cpt&ref_t=design&ref_c=273d9530-1b98-4bbe-9c39-dd79889a6e16",
    AOI_RS: "http://10.73.17.17:8080/webroot/decision/view/report?viewlet=Yield_Monitoring%252FDefect_Monitoring%252FDB114.RS_Density_Analyze_for_BJX_Report.cpt&ref_t=design&op=view&ref_c=5984e028-78ba-4264-857b-d9ee3d00fcca",
    CTQ: "http://10.73.17.76:8080/webroot/decision/v10/entry/access/6883bfc0-b11b-4548-8e78-3b6a3474f0d0?preview=true",
    QTIME: "http://10.73.17.17:8080/webroot/decision/view/report?viewlet=Test%252FQTIME_TZBJX.cpt&ref_t=design&ref_c=273d9530-1b98-4bbe-9c39-dd79889a6e16",
    IQC: "http://10.73.17.17:8080/webroot/decision/view/report?viewlet=Quality_Monitoring%252FPQC%252FI03_Search_TianZhu_IQC_Mateial_OKNG_Ratel.cpt&ref_t=design&op=view&ref_c=4b854cd4-c836-4724-ba9d-0ad9a72584e5"
};

const APP_CONFIG = {
    // ============================================================
    // 0. 核心节点链接 (Core Node) - 已改为占位，自动预警移至分支节点
    // ============================================================
    coreUrl: "#",
    
    // 新增：自动预警看板独立节点配置
    warningNode: {
        id: 'warning',
        label: '自动预警',
        icon: '<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line>',
        url: LINKS.WARNING_DASHBOARD
    },
    
    // ============================================================
    // 1. 侧边栏菜单 (Sidebar) - 使用上面的变量
    // ============================================================
    sidebarLinks: [
            
        // --- 分组链接 (有 children) ---
        { 
            name: "入库不良率", 
            // 有这个 children 字段，就会渲染成折叠菜单
            children: [
                { name: "入库不良率ByLot明细表", url: LINKS.YIELD_BY_LOT },
                { name: "入库不良率BySheet明细表", url: LINKS.YIELD_BY_SHEET },
                { name: "入库不良率分析看板", url: LINKS.YIELD_TREND_MWD },
            ]
        },
        {
            name: "Q-time",
            children: [
                { name: "Q-time", url: LINKS.QTIME },
            ]
        },
        {
            name: "SPC监控",
            children: [
                { name: "AOI", url: LINKS.AOI },
                { name: "AOI_RS", url: LINKS.AOI_RS },
                { name: "SPC", url: LINKS.SPC },
                { name: "CTQ", url: LINKS.CTQ },
            ]
        },
        {
            name: "解析资料",
            children: [
                { name: "解析资料", url: LINKS.ANALYSIS_FILES },
            ]
        },
        {
            name: "专项资料",
            children: [
                { name: "台账周报", url: LINKS.PROJECT_FILES },
            ]
        },
        {
            name: "IQC监控",
            children: [
                { name: "IQC", url: LINKS.IQC },
            ]
        },
    ],

    // ============================================================
    // 2. 技能树节点 (Skill Tree) - 同样使用上面的变量 (复用！)
    // ============================================================
    treeData: [
        { 
            id: 'rate_report', 
            label: '入库不良率', 
            icon: '<ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>', 
            subs: [ 
                {l:'', v:'Lot明细', url: LINKS.YIELD_BY_LOT },  
                {l:'', v:'Sheet明细', url: LINKS.YIELD_BY_SHEET }, 
                {l:'', v:'不良率分析', url: LINKS.YIELD_TREND_MWD },   
            ] 
        },
        { 
            id: 'spc', 
            label: 'SPC监控', 
            icon: '<path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>', 
            subs: [ 
                {l:'', v:'AOI', url: LINKS.AOI } ,
                {l:'', v:'AOI_RS', url: LINKS.AOI_RS },
                {l:'', v:'SPC', url: LINKS.SPC }, 
                {l:'', v:'CTQ', url: LINKS.CTQ },
            ]
        },
        { 
            id: 'Q-time', 
            label: 'Q-time监控', 
            icon: '<path d="M22 12h-4l-3 9L9 3l-3 9H2"></path>', 
            subs: [ 
                {l:'', v:'Q-time', url: LINKS.QTIME },          
                     
            ]
        },
        { 
            id: '解析资料', 
            label: '解析资料', 
            icon: '<rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="3" y1="9" x2="21" y2="9"></line><line x1="9" y1="21" x2="9" y2="9"></line>', 
            subs: [ 
                {l:'', v:'解析报告', url: LINKS.ANALYSIS_FILES } 
            ]
        },
        { 
            id: '专项资料', 
            label: '专项资料', 
            icon: '<path d="M12 15.5A3.5 3.5 0 0 1 8.5 12A3.5 3.5 0 0 1 12 8.5a3.5 3.5 0 0 1 3.5 3.5a3.5 3.5 0 0 1-3.5 3.5m7.43-2.53c.04-.32.07-.64.07-.97c0-.33-.03-.65-.07-.97l2.11-1.63c.19-.15.24-.42.12-.64l-2-3.46c-.12-.22-.39-.3-.61-.22l-2.49 1c-.52-.39-1.06-.73-1.69-.98l-.37-2.65A.506.506 0 0 0 14 2h-4c-.25 0-.46.18-.5.42l-.37 2.65c-.63.25-1.17.59-1.69.98l-2.49-1c-.22-.08-.49 0-.61.22l-2 3.46c-.13.22-.07.49.12.64L4.57 11c-.04.32-.07.64-.07.97c0 .33.03.65.07.97l-2.11 1.63c-.19.15-.24.42-.12.64l2 3.46c.12.22.39.3.61.22l2.49-1c.52.39 1.06.73 1.69.98l.37 2.65c.04.24.25.42.5.42h4c.25 0 .46-.18.5-.42l.37-2.65c.63-.25 1.17-.59 1.69-.98l2.49 1c.22.08.49 0 .61-.22l2-3.46c.13-.22.07-.49-.12-.64l-2.11-1.63Z"></path>',
            subs: [ 
                {l:'', v:'台账周报', url: LINKS.PROJECT_FILES } 
            ]
        },
        { 
            id: 'IQC', 
            label: 'IQC', 
            icon: '<path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"></path>', 
            subs: [ 
                {l:'', v:'IQC', url: LINKS.IQC }          
            ]
        },
        // 新增：自动预警独立分支节点
        { 
            id: 'warning', 
            label: '自动预警', 
            icon: '<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line>', 
            subs: [ 
                {l:'', v:'预警看板', url: LINKS.WARNING_DASHBOARD }
            ]
        },
    ]
};