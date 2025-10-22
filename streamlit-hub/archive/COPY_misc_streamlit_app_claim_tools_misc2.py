#!/usr/bin/env python3
"""
Professional Claim Analysis Streamlit App
Minimalistic, prestigious design with uniform fonts
"""

import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
import warnings

# Suppress pandas warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# Custom CSS for professional styling
st.markdown("""
<style>
    /* Import professional font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global styles */
    .main {
        padding-top: 2rem;
    }
    
    /* Header styling */
    .main-header {
        font-family: 'Inter', sans-serif;
        font-size: 2.5rem;
        font-weight: 700;
        color: #1a1a1a;
        text-align: center;
        margin-bottom: 0.5rem;
        letter-spacing: -0.02em;
    }
    
    .sub-header {
        font-family: 'Inter', sans-serif;
        font-size: 1rem;
        font-weight: 400;
        color: #666666;
        text-align: center;
        margin-bottom: 3rem;
    }
    
    /* Section headers */
    .section-header {
        font-family: 'Inter', sans-serif;
        font-size: 1.5rem;
        font-weight: 600;
        color: #1a1a1a;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #e5e5e5;
        padding-bottom: 0.5rem;
    }
    
    .subsection-header {
        font-family: 'Inter', sans-serif;
        font-size: 1.25rem;
        font-weight: 500;
        color: #333333;
        margin-top: 1.5rem;
        margin-bottom: 0.75rem;
    }
    
    /* Card styling */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e5e5e5;
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .metric-label {
        font-family: 'Inter', sans-serif;
        font-size: 0.875rem;
        font-weight: 500;
        color: #666666;
        margin-bottom: 0.25rem;
    }
    
    .metric-value {
        font-family: 'Inter', sans-serif;
        font-size: 1.5rem;
        font-weight: 600;
        color: #1a1a1a;
    }
    
    .metric-value.critical {
        color: #dc2626;
    }
    
    .metric-value.high {
        color: #ea580c;
    }
    
    .metric-value.medium {
        color: #d97706;
    }
    
    .metric-value.low {
        color: #059669;
    }
    
    /* Status badges */
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-family: 'Inter', sans-serif;
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .status-critical {
        background: #fef2f2;
        color: #dc2626;
        border: 1px solid #fecaca;
    }
    
    .status-high {
        background: #fff7ed;
        color: #ea580c;
        border: 1px solid #fed7aa;
    }
    
    .status-medium {
        background: #fffbeb;
        color: #d97706;
        border: 1px solid #fde68a;
    }
    
    .status-low {
        background: #f0fdf4;
        color: #059669;
        border: 1px solid #bbf7d0;
    }
    
    /* Issue list styling */
    .issue-item {
        background: #ffffff;
        border: 1px solid #e5e5e5;
        border-radius: 6px;
        padding: 1rem;
        margin-bottom: 0.75rem;
        font-family: 'Inter', sans-serif;
    }
    
    .issue-header {
        font-size: 0.875rem;
        font-weight: 600;
        color: #1a1a1a;
        margin-bottom: 0.5rem;
    }
    
    .issue-details {
        font-size: 0.8rem;
        color: #666666;
        line-height: 1.4;
    }
    
    .issue-risk {
        font-size: 0.75rem;
        font-weight: 500;
        margin-top: 0.5rem;
    }
    
    /* Fix list styling */
    .fix-item {
        background: #f8fafc;
        border-left: 4px solid #3b82f6;
        padding: 1rem;
        margin-bottom: 0.75rem;
        font-family: 'Inter', sans-serif;
    }
    
    .fix-header {
        font-size: 0.875rem;
        font-weight: 600;
        color: #1a1a1a;
        margin-bottom: 0.5rem;
    }
    
    .fix-details {
        font-size: 0.8rem;
        color: #4b5563;
        line-height: 1.4;
    }
    
    /* Recommendation box */
    .recommendation-box {
        background: #f0f9ff;
        border: 1px solid #0ea5e9;
        border-radius: 8px;
        padding: 1.5rem;
        margin: 1.5rem 0;
        font-family: 'Inter', sans-serif;
    }
    
    .recommendation-text {
        font-size: 1rem;
        font-weight: 500;
        color: #0c4a6e;
        margin: 0;
    }
    
    /* Hide streamlit default elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Custom sidebar */
    .sidebar .sidebar-content {
        background: #f8fafc;
    }
</style>
""", unsafe_allow_html=True)

class ClaimAnalysisApp:
    def __init__(self):
        # Database connection
        self.server = "localhost,1433"
        self.database = "_reporting"
        self.username = "SA"
        self.password = "Bbanwo@1980!"
        
        self.conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={self.server};Database={self.database};"
            f"UID={self.username};PWD={self.password};"
            "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
        )
    
    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.conn_str)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return None
    
    def get_claim_summary(self, claim_id):
        """Get high-level claim summary"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
            SELECT TOP 1
                CLM_ID,
                DESYNPUF_ID,
                clm_from_dt,
                clm_thru_dt,
                PRVDR_NUM,
                COUNT(*) as total_combinations,
                COUNT(DISTINCT hcpcs_code) as unique_procedures,
                COUNT(DISTINCT icd9_dgns_code) as unique_diagnoses,
                MAX(denial_risk_score) as max_risk_score,
                AVG(denial_risk_score) as avg_risk_score,
                SUM(CASE WHEN denial_risk_level LIKE '%CRITICAL%' THEN 1 ELSE 0 END) as critical_issues,
                SUM(CASE WHEN denial_risk_level LIKE '%HIGH%' THEN 1 ELSE 0 END) as high_issues,
                SUM(CASE WHEN denial_risk_level LIKE '%MEDIUM%' THEN 1 ELSE 0 END) as medium_issues,
                SUM(CASE WHEN denial_risk_level LIKE '%LOW%' THEN 1 ELSE 0 END) as low_issues,
                SUM(CASE WHEN denial_risk_level = 'OK' THEN 1 ELSE 0 END) as ok_combinations
            FROM [_reporting].[dbo].[vw_Enhanced_Claims_Risk_Analysis]
            WHERE CLM_ID = ?
            GROUP BY CLM_ID, DESYNPUF_ID, clm_from_dt, clm_thru_dt, PRVDR_NUM
            """
            
            df = pd.read_sql(query, conn, params=[claim_id])
            
            if df.empty:
                return None
            
            return df.iloc[0].to_dict()
            
        except Exception as e:
            st.error(f"Failed to retrieve claim summary: {e}")
            return None
        finally:
            conn.close()
    
    def get_claim_details(self, claim_id):
        """Get detailed claim analysis with deduplication"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
            SELECT 
                dx_position,
                icd9_dgns_code,
                mapped_icd10_code,
                hcpcs_position,
                hcpcs_code,
                denial_risk_level,
                denial_risk_score,
                risk_category,
                action_required,
                business_impact,
                ptp_denial_reason,
                mue_denial_type,
                ncd_title
            FROM [_reporting].[dbo].[vw_Enhanced_Claims_Risk_Analysis]
            WHERE CLM_ID = ?
            ORDER BY 
                CASE 
                    WHEN denial_risk_level LIKE '%CRITICAL%' THEN 1
                    WHEN denial_risk_level LIKE '%HIGH%' THEN 2
                    WHEN denial_risk_level LIKE '%MEDIUM%' THEN 3
                    WHEN denial_risk_level LIKE '%LOW%' THEN 4
                    ELSE 5
                END,
                denial_risk_score DESC
            """
            
            df = pd.read_sql(query, conn, params=[claim_id])
            
            # Remove duplicates using pandas
            df_deduped = df.drop_duplicates(subset=['dx_position', 'icd9_dgns_code', 'hcpcs_position', 'hcpcs_code', 'denial_risk_level'])
            
            return df_deduped
            
        except Exception as e:
            st.error(f"Failed to retrieve claim details: {e}")
            return None
        finally:
            conn.close()
    
    def get_actionable_fixes(self, claim_id):
        """Get specific actionable fixes with deduplication"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            query = """
            SELECT 
                dx_position,
                icd9_dgns_code,
                mapped_icd10_code,
                hcpcs_position,
                hcpcs_code,
                denial_risk_level,
                denial_risk_score,
                risk_category,
                action_required,
                business_impact,
                ptp_denial_reason,
                mue_denial_type
            FROM [_reporting].[dbo].[vw_Enhanced_Claims_Risk_Analysis]
            WHERE CLM_ID = ? 
            AND denial_risk_level != 'OK'
            ORDER BY 
                CASE 
                    WHEN denial_risk_level LIKE '%CRITICAL%' THEN 1
                    WHEN denial_risk_level LIKE '%HIGH%' THEN 2
                    WHEN denial_risk_level LIKE '%MEDIUM%' THEN 3
                    WHEN denial_risk_level LIKE '%LOW%' THEN 4
                    ELSE 5
                END,
                denial_risk_score DESC
            """
            
            df = pd.read_sql(query, conn, params=[claim_id])
            
            # Remove duplicates using pandas
            df_deduped = df.drop_duplicates(subset=['dx_position', 'icd9_dgns_code', 'hcpcs_position', 'hcpcs_code', 'denial_risk_level'])
            
            return df_deduped
            
        except Exception as e:
            st.error(f"Failed to retrieve actionable fixes: {e}")
            return None
        finally:
            conn.close()
    
    def get_risk_class(self, risk_score):
        """Get risk classification"""
        if risk_score >= 100:
            return "critical"
        elif risk_score >= 80:
            return "high"
        elif risk_score >= 50:
            return "medium"
        else:
            return "low"
    
    def get_status_class(self, risk_level):
        """Get status badge class"""
        if "CRITICAL" in risk_level:
            return "status-critical"
        elif "HIGH" in risk_level:
            return "status-high"
        elif "MEDIUM" in risk_level:
            return "status-medium"
        else:
            return "status-low"
    
    def render_app(self):
        """Render the Streamlit app"""
        # Header
        st.markdown('<h1 class="main-header">Claim Analysis Dashboard</h1>', unsafe_allow_html=True)
        st.markdown('<p class="sub-header">Professional Medicare Claim Risk Assessment</p>', unsafe_allow_html=True)
        
        # Sidebar for input
        with st.sidebar:
            st.markdown("### Analysis Parameters")
            claim_id = st.text_input("Claim ID", value="542052281361022", help="Enter the claim ID to analyze")
            
            if st.button("Analyze Claim", type="primary"):
                st.session_state.analyze_claim = True
                st.session_state.claim_id = claim_id
        
        # Main content
        if hasattr(st.session_state, 'analyze_claim') and st.session_state.analyze_claim:
            claim_id = st.session_state.claim_id
            
            try:
                claim_id = int(claim_id)
            except ValueError:
                st.error("Please enter a valid claim ID")
                return
            
            # Get data
            with st.spinner("Analyzing claim..."):
                summary = self.get_claim_summary(claim_id)
                
                if not summary:
                    st.error("No data found for this claim ID")
                    return
                
                details = self.get_claim_details(claim_id)
                fixes = self.get_actionable_fixes(claim_id)
            
            # Claim Summary Section
            st.markdown('<h2 class="section-header">Claim Summary</h2>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Provider</div>
                    <div class="metric-value">{summary['PRVDR_NUM']}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col2:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Service Date</div>
                    <div class="metric-value">{summary['clm_from_dt']}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col3:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Patient ID</div>
                    <div class="metric-value">{summary['DESYNPUF_ID']}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            # Risk Assessment
            st.markdown('<h2 class="section-header">Risk Assessment</h2>', unsafe_allow_html=True)
            
            max_risk = summary['max_risk_score']
            avg_risk = summary['avg_risk_score']
            
            if max_risk >= 100:
                decision = "DENY"
                priority = "CRITICAL"
            elif max_risk >= 80:
                decision = "REVIEW"
                priority = "HIGH"
            elif max_risk >= 50:
                decision = "MONITOR"
                priority = "MEDIUM"
            else:
                decision = "APPROVE"
                priority = "LOW"
            
            risk_class = self.get_risk_class(max_risk)
            status_class = self.get_status_class(priority)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Decision</div>
                    <div class="metric-value {risk_class}">{decision}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col2:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Priority</div>
                    <div class="metric-value {risk_class}">{priority}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col3:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Max Risk Score</div>
                    <div class="metric-value {risk_class}">{max_risk}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            with col4:
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label">Avg Risk Score</div>
                    <div class="metric-value {risk_class}">{avg_risk:.1f}</div>
                </div>
                ''', unsafe_allow_html=True)
            
            # Issue Breakdown
            st.markdown('<h2 class="section-header">Issue Breakdown</h2>', unsafe_allow_html=True)
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Total Combinations", f"{summary['total_combinations']:,}")
            with col2:
                st.metric("Unique Procedures", summary['unique_procedures'])
            with col3:
                st.metric("Unique Diagnoses", summary['unique_diagnoses'])
            with col4:
                st.metric("OK Combinations", summary['ok_combinations'])
            with col5:
                st.metric("Issues Found", 
                         summary['critical_issues'] + summary['high_issues'] + 
                         summary['medium_issues'] + summary['low_issues'])
            
            # Issue Summary
            st.markdown('<h2 class="section-header">Issue Summary</h2>', unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Critical Issues", summary['critical_issues'], 
                         delta=None, help="Must fix before submission")
            with col2:
                st.metric("High Issues", summary['high_issues'], 
                         delta=None, help="Review before submission")
            with col3:
                st.metric("Medium Issues", summary['medium_issues'], 
                         delta=None, help="Monitor after submission")
            with col4:
                st.metric("Low Issues", summary['low_issues'], 
                         delta=None, help="Minor issues to monitor")
            
            # Action Required
            st.markdown('<h2 class="section-header">Action Required</h2>', unsafe_allow_html=True)
            
            if summary['critical_issues'] > 0:
                action = "IMMEDIATE: Fix critical issues or claim will be denied"
                impact = "HIGH: Full denial risk due to critical issues"
            elif summary['high_issues'] > 0:
                action = "REVIEW: Address high-risk issues before submission"
                impact = "MEDIUM: Partial denial risk"
            elif summary['medium_issues'] > 0:
                action = "MONITOR: Review medium-risk issues"
                impact = "LOW: Minor issues to monitor"
            else:
                action = "NO ACTION: Claim appears compliant"
                impact = "NO IMPACT: Claim should process normally"
            
            st.markdown(f'''
            <div class="recommendation-box">
                <p class="recommendation-text"><strong>Action Required:</strong> {action}</p>
                <p class="recommendation-text"><strong>Business Impact:</strong> {impact}</p>
            </div>
            ''', unsafe_allow_html=True)
            
            # Detailed Issues
            if details is not None and not details.empty:
                st.markdown('<h2 class="section-header">Detailed Issue Analysis</h2>', unsafe_allow_html=True)
                
                # Group by risk level
                critical_issues = details[details['denial_risk_level'].str.contains('CRITICAL', na=False)]
                high_issues = details[details['denial_risk_level'].str.contains('HIGH', na=False)]
                medium_issues = details[details['denial_risk_level'].str.contains('MEDIUM', na=False)]
                low_issues = details[details['denial_risk_level'].str.contains('LOW', na=False)]
                
                if not critical_issues.empty:
                    st.markdown('<h3 class="subsection-header">Critical Issues (Must Fix)</h3>', unsafe_allow_html=True)
                    for idx, row in critical_issues.head(10).iterrows():
                        status_class = self.get_status_class(row['denial_risk_level'])
                        st.markdown(f'''
                        <div class="issue-item">
                            <div class="issue-header">
                                DX {row['dx_position']}: {row['icd9_dgns_code']} → {row['mapped_icd10_code']} | 
                                Procedure {row['hcpcs_position']}: {row['hcpcs_code']}
                            </div>
                            <div class="issue-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}<br>
                                <strong>Action:</strong> {row['action_required']}
                            </div>
                            <div class="issue-risk">
                                <span class="status-badge {status_class}">Risk Score: {row['denial_risk_score']}</span>
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                
                if not high_issues.empty:
                    st.markdown('<h3 class="subsection-header">High Risk Issues</h3>', unsafe_allow_html=True)
                    for idx, row in high_issues.head(10).iterrows():
                        status_class = self.get_status_class(row['denial_risk_level'])
                        st.markdown(f'''
                        <div class="issue-item">
                            <div class="issue-header">
                                DX {row['dx_position']}: {row['icd9_dgns_code']} → {row['mapped_icd10_code']} | 
                                Procedure {row['hcpcs_position']}: {row['hcpcs_code']}
                            </div>
                            <div class="issue-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}
                            </div>
                            <div class="issue-risk">
                                <span class="status-badge {status_class}">Risk Score: {row['denial_risk_score']}</span>
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                
                if not medium_issues.empty:
                    st.markdown('<h3 class="subsection-header">Medium Risk Issues</h3>', unsafe_allow_html=True)
                    for idx, row in medium_issues.head(5).iterrows():
                        status_class = self.get_status_class(row['denial_risk_level'])
                        st.markdown(f'''
                        <div class="issue-item">
                            <div class="issue-header">
                                DX {row['dx_position']}: {row['icd9_dgns_code']} → {row['mapped_icd10_code']} | 
                                Procedure {row['hcpcs_position']}: {row['hcpcs_code']}
                            </div>
                            <div class="issue-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}
                            </div>
                            <div class="issue-risk">
                                <span class="status-badge {status_class}">Risk Score: {row['denial_risk_score']}</span>
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                
                if not low_issues.empty:
                    st.markdown('<h3 class="subsection-header">Low Risk Issues</h3>', unsafe_allow_html=True)
                    for idx, row in low_issues.head(3).iterrows():
                        status_class = self.get_status_class(row['denial_risk_level'])
                        st.markdown(f'''
                        <div class="issue-item">
                            <div class="issue-header">
                                DX {row['dx_position']}: {row['icd9_dgns_code']} → {row['mapped_icd10_code']} | 
                                Procedure {row['hcpcs_position']}: {row['hcpcs_code']}
                            </div>
                            <div class="issue-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}
                            </div>
                            <div class="issue-risk">
                                <span class="status-badge {status_class}">Risk Score: {row['denial_risk_score']}</span>
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
            
            # Specific Fixes
            if fixes is not None and not fixes.empty:
                st.markdown('<h2 class="section-header">Specific Fixes Needed</h2>', unsafe_allow_html=True)
                
                # Group by risk level
                critical_fixes = fixes[fixes['denial_risk_level'].str.contains('CRITICAL', na=False)]
                high_fixes = fixes[fixes['denial_risk_level'].str.contains('HIGH', na=False)]
                medium_fixes = fixes[fixes['denial_risk_level'].str.contains('MEDIUM', na=False)]
                low_fixes = fixes[fixes['denial_risk_level'].str.contains('LOW', na=False)]
                
                fix_count = 1
                
                if not critical_fixes.empty:
                    st.markdown('<h3 class="subsection-header">Critical Fixes (Must Fix Before Submission)</h3>', unsafe_allow_html=True)
                    for idx, row in critical_fixes.head(10).iterrows():
                        # Generate specific fix
                        if "Primary DX Not Covered" in row['denial_risk_level']:
                            fix = f"Replace primary diagnosis {row['icd9_dgns_code']} with a diagnosis that justifies procedure {row['hcpcs_code']}"
                        elif "MUE Risk" in row['denial_risk_level']:
                            fix = f"Verify documentation supports units billed for procedure {row['hcpcs_code']}"
                        elif "PTP Conflict" in row['denial_risk_level']:
                            fix = f"Check if procedures {row['hcpcs_code']} can be billed together"
                        elif "NCCI PTP Conflict" in row['denial_risk_level']:
                            fix = f"Remove conflicting procedure or verify they can be billed together"
                        else:
                            fix = row['action_required']
                        
                        st.markdown(f'''
                        <div class="fix-item">
                            <div class="fix-header">
                                {fix_count}. DIAGNOSIS {row['icd9_dgns_code']} → PROCEDURE {row['hcpcs_code']}
                            </div>
                            <div class="fix-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}<br>
                                <strong>Risk Score:</strong> {row['denial_risk_score']}<br>
                                <strong>Fix:</strong> {fix}<br>
                                <strong>Impact:</strong> {row['business_impact']}
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                        fix_count += 1
                
                if not high_fixes.empty:
                    st.markdown('<h3 class="subsection-header">High Priority Fixes</h3>', unsafe_allow_html=True)
                    for idx, row in high_fixes.head(5).iterrows():
                        st.markdown(f'''
                        <div class="fix-item">
                            <div class="fix-header">
                                {fix_count}. DIAGNOSIS {row['icd9_dgns_code']} → PROCEDURE {row['hcpcs_code']}
                            </div>
                            <div class="fix-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}<br>
                                <strong>Risk Score:</strong> {row['denial_risk_score']}<br>
                                <strong>Fix:</strong> {row['action_required']}
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                        fix_count += 1
                
                if not medium_fixes.empty:
                    st.markdown('<h3 class="subsection-header">Medium Priority Fixes</h3>', unsafe_allow_html=True)
                    for idx, row in medium_fixes.head(3).iterrows():
                        st.markdown(f'''
                        <div class="fix-item">
                            <div class="fix-header">
                                {fix_count}. DIAGNOSIS {row['icd9_dgns_code']} → PROCEDURE {row['hcpcs_code']}
                            </div>
                            <div class="fix-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}<br>
                                <strong>Risk Score:</strong> {row['denial_risk_score']}<br>
                                <strong>Fix:</strong> {row['action_required']}
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                        fix_count += 1
                
                if not low_fixes.empty:
                    st.markdown('<h3 class="subsection-header">Low Priority Fixes</h3>', unsafe_allow_html=True)
                    for idx, row in low_fixes.head(2).iterrows():
                        st.markdown(f'''
                        <div class="fix-item">
                            <div class="fix-header">
                                {fix_count}. DIAGNOSIS {row['icd9_dgns_code']} → PROCEDURE {row['hcpcs_code']}
                            </div>
                            <div class="fix-details">
                                <strong>Issue:</strong> {row['denial_risk_level']}<br>
                                <strong>Risk Score:</strong> {row['denial_risk_score']}<br>
                                <strong>Fix:</strong> {row['action_required']}
                            </div>
                        </div>
                        ''', unsafe_allow_html=True)
                        fix_count += 1
            
            # Footer
            st.markdown("---")
            st.markdown(f"*Analysis completed on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

def main():
    app = ClaimAnalysisApp()
    app.render_app()

if __name__ == "__main__":
    main()




