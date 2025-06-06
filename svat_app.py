# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="Snowflake Validation Automation Tool",
    page_icon="❄️",
    layout="wide"
)

# ========== SNOWFLAKE FUNCTIONS ==========
def get_snowflake_connection(user, password, account):
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            authenticator='snowflake'
        )
        return conn, "✅ Successfully connected!"
    except Exception as e:
        return None, f"❌ Connection failed: {str(e)}"

def disconnect_snowflake(conn):
    """Close Snowflake connection"""
    if conn:
        conn.close()
    return None, "🔌 Disconnected successfully"

def get_databases(conn):
    """Get list of databases"""
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        st.error(f"Error getting databases: {str(e)}")
        return []

def get_schemas(conn, database):
    """Get schemas for specific database"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[1] for row in cursor.fetchall()]
    except Exception as e:
        st.error(f"Error getting schemas: {str(e)}")
        return []

def clone_schema(conn, source_db, source_schema, target_schema):
    """Clone schema with improved error handling and reporting"""
    cursor = conn.cursor()
    try:
        # First check if source schema exists
        cursor.execute(f"SHOW SCHEMAS LIKE '{source_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Source schema {source_db}.{source_schema} doesn't exist", pd.DataFrame()
        
        # Execute clone command
        cursor.execute(
            f"CREATE OR REPLACE SCHEMA {source_db}.{target_schema} "
            f"CLONE {source_db}.{source_schema}"
        )
        
        # Verify clone was successful
        cursor.execute(f"SHOW SCHEMAS LIKE '{target_schema}' IN DATABASE {source_db}")
        if not cursor.fetchall():
            return False, f"❌ Clone failed - target schema not created", pd.DataFrame()
        
        # Get list of cloned tables
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{source_schema}")
        source_tables = [row[1] for row in cursor.fetchall()]
        
        cursor.execute(f"SHOW TABLES IN SCHEMA {source_db}.{target_schema}")
        clone_tables = [row[1] for row in cursor.fetchall()]
        
        # Create summary DataFrame
        df_tables = pd.DataFrame({
            'Database': source_db,
            'Source Schema': source_schema,
            'Clone Schema': target_schema,
            'Source Tables': len(source_tables),
            'Cloned Tables': len(clone_tables),
            'Status': '✅ Success' if len(source_tables) == len(clone_tables) else '⚠️ Partial Success'
        }, index=[0])
        
        return True, f"✅ Successfully cloned {source_db}.{source_schema} to {source_db}.{target_schema}", df_tables
    except Exception as e:
        return False, f"❌ Clone failed: {str(e)}", pd.DataFrame()

def compare_table_differences(conn, db_name, source_schema, clone_schema):
    """Compare tables between schemas"""
    cursor = conn.cursor()
    
    query = f"""
    WITH source_tables AS (
        SELECT table_name 
        FROM {db_name}.information_schema.tables 
        WHERE table_schema = '{source_schema}'
    ),
    clone_tables AS (
        SELECT table_name 
        FROM {db_name}.information_schema.tables 
        WHERE table_schema = '{clone_schema}'
    )
    SELECT 
        COALESCE(s.table_name, c.table_name) AS table_name,
        CASE 
            WHEN s.table_name IS NULL THEN 'Missing in source - Table Dropped'
            WHEN c.table_name IS NULL THEN 'Missing in clone - Table Added'
            ELSE 'Present in both'
        END AS difference
    FROM source_tables s
    FULL OUTER JOIN clone_tables c ON s.table_name = c.table_name
    WHERE s.table_name IS NULL OR c.table_name IS NULL
    ORDER BY difference, table_name;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    return pd.DataFrame(results, columns=['Table Name', 'Difference'])

def compare_column_differences(conn, db_name, source_schema, clone_schema):
    """Compare columns and data types between schemas"""
    cursor = conn.cursor()
    
    # Get common tables
    common_tables_query = f"""
    SELECT s.table_name
    FROM {db_name}.information_schema.tables s
    JOIN {db_name}.information_schema.tables c 
        ON s.table_name = c.table_name
    WHERE s.table_schema = '{source_schema}'
    AND c.table_schema = '{clone_schema}';
    """
    
    cursor.execute(common_tables_query)
    common_tables = [row[0] for row in cursor.fetchall()]
    
    column_diff_data = []
    datatype_diff_data = []
    
    for table in common_tables:
        # Get source table description
        cursor.execute(f"DESCRIBE TABLE {db_name}.{source_schema}.{table}")
        source_desc = cursor.fetchall()
        source_cols = {row[0]: row[1] for row in source_desc}
        
        # Get clone table description
        cursor.execute(f"DESCRIBE TABLE {db_name}.{clone_schema}.{table}")
        clone_desc = cursor.fetchall()
        clone_cols = {row[0]: row[1] for row in clone_desc}
        
        # Get all unique column names
        all_columns = set(source_cols.keys()).union(set(clone_cols.keys()))
        
        for col in all_columns:
            source_exists = col in source_cols
            clone_exists = col in clone_cols
            
            if not source_exists:
                column_diff_data.append({
                    'Table': table,
                    'Column': col,
                    'Difference': 'Missing in source - Column Dropped',
                    'Source Data Type': None,
                    'Clone Data Type': clone_cols.get(col)
                })
            elif not clone_exists:
                column_diff_data.append({
                    'Table': table,
                    'Column': col,
                    'Difference': 'Missing in clone - Column Added',
                    'Source Data Type': source_cols.get(col),
                    'Clone Data Type': None
                })
            else:
                # Column exists in both - check data type
                if source_cols[col] != clone_cols[col]:
                    datatype_diff_data.append({
                        'Table': table,
                        'Column': col,
                        'Source Data Type': source_cols[col],
                        'Clone Data Type': clone_cols[col],
                        'Message': 'Data Type Changed'
                    })
    
    # Create DataFrames
    column_diff_df = pd.DataFrame(column_diff_data)
    if not column_diff_df.empty:
        column_diff_df = column_diff_df[['Table', 'Column', 'Difference', 'Source Data Type', 'Clone Data Type']]
    
    datatype_diff_df = pd.DataFrame(datatype_diff_data)
    if not datatype_diff_df.empty:
        datatype_diff_df = datatype_diff_df[['Table', 'Column', 'Source Data Type', 'Clone Data Type', 'Message']]
    
    return column_diff_df, datatype_diff_df

def validate_kpis(conn, database, source_schema, target_schema):
    """Validate KPIs between source and clone schemas"""
    cursor = conn.cursor()
    results = []
    
    try:
        # Fetch all KPIs from ORDER_KPIS table
        cursor.execute(f"SELECT KPI_ID, KPI_NAME, KPI_VALUE FROM {database}.{source_schema}.ORDER_KPIS")
        kpis = cursor.fetchall()
        
        if not kpis:
            return pd.DataFrame(), "⚠️ No KPIs found in the source schema"
        
        for kpi_id, kpi_name, kpi_query in kpis:
            try:
                # Execute against source schema
                source_query = kpi_query.replace('ORDER_DATA', f'{database}.{source_schema}.ORDER_DATA')
                cursor.execute(source_query)
                result_source = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                
                # Execute against target schema
                clone_query = kpi_query.replace('ORDER_DATA', f'{database}.{target_schema}.ORDER_DATA')
                cursor.execute(clone_query)
                result_clone = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                
                # Calculate differences
                diff = None
                pct_diff = None
                
                if result_source is not None and result_clone is not None:
                    try:
                        num_source = float(result_source)
                        num_clone = float(result_clone)
                        diff = num_source - num_clone
                        pct_diff = (diff / num_source) * 100 if num_source != 0 else float('inf')
                    except (ValueError, TypeError):
                        pass
                
                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Source Value': result_source,
                    'Clone Value': result_clone,
                    'Difference': diff if diff is not None else "N/A",
                    'Difference %': f"{pct_diff:.2f}%" if pct_diff is not None else "N/A",
                    'Status': '✅ Match' if result_source == result_clone else '⚠️ Mismatch'
                })
                
            except Exception as e:
                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Source Value': f"Error: {str(e)}",
                    'Clone Value': "",
                    'Difference': "",
                    'Difference %': "",
                    'Status': '❌ Error'
                })
        
        df = pd.DataFrame(results)
        return df, "✅ KPI validation completed"
    
    except Exception as e:
        return pd.DataFrame(), f"❌ KPI validation failed: {str(e)}"

# ========== STREAMLIT UI ==========
def main():
    # Add company logo and title
    st.image("LOGO_URL", width=200)
    st.title("Snowflake Validation Automation Tool")
    
    # Initialize session state
    if 'conn' not in st.session_state:
        st.session_state.conn = None
    if 'login_success' not in st.session_state:
        st.session_state.login_success = False
    
    # ===== LOGIN SECTION =====
    with st.expander("🔐 Login", expanded=not st.session_state.login_success):
        if not st.session_state.login_success:
            with st.form("login_form"):
                st.subheader("Snowflake Connection")
                user = st.text_input("Username", placeholder="your_username")
                password = st.text_input("Password", type="password", placeholder="••••••••")
                account = st.text_input("Account", placeholder="account.region")
                
                if st.form_submit_button("Connect"):
                    with st.spinner("Connecting to Snowflake..."):
                        conn, msg = get_snowflake_connection(user, password, account)
                        if conn:
                            st.session_state.conn = conn
                            st.session_state.login_success = True
                            st.session_state.user = user
                            st.session_state.account = account
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
        else:
            st.success(f"✅ Connected as {st.session_state.user} to {st.session_state.account}")
            if st.button("Disconnect"):
                with st.spinner("Disconnecting..."):
                    _, msg = disconnect_snowflake(st.session_state.conn)
                    st.session_state.conn = None
                    st.session_state.login_success = False
                    st.success(msg)
                    st.rerun()
    
    # ===== MAIN APP =====
    if st.session_state.login_success:
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["⎘ Clone", "🔍 Schema Validation", "📊 KPI Validation"])
        
        # ===== CLONE TAB =====
        with tab1:
            st.subheader("Schema Clone")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Source Selection")
                source_db = st.selectbox(
                    "Source Database",
                    get_databases(st.session_state.conn),
                    index=0
                )
                source_schema = st.selectbox(
                    "Source Schema",
                    get_schemas(st.session_state.conn, source_db),
                    index=0
                )
                target_schema = st.text_input("Target Schema Name")
                
                if st.button("Execute Clone", type="primary"):
                    with st.spinner("Cloning schema..."):
                        success, message, df = clone_schema(
                            st.session_state.conn, source_db, source_schema, target_schema
                        )
                        if success:
                            st.success(message)
                            st.json({
                                "source": f"{source_db}.{source_schema}",
                                "target": f"{source_db}.{target_schema}",
                                "timestamp": datetime.now().isoformat(),
                                "status": "success",
                                "tables_cloned": int(df['Cloned Tables'].iloc[0]) if not df.empty else 0
                            })
                            st.dataframe(df)
                        else:
                            st.error(message)
            
            with col2:
                st.markdown("### Clone Status")
                st.info("Enter clone details on the left and click 'Execute Clone'")
        
        # ===== SCHEMA VALIDATION TAB =====
        with tab2:
            st.subheader("Schema Validation")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Validation Configuration")
                val_db = st.selectbox(
                    "Database",
                    get_databases(st.session_state.conn),
                    index=0,
                    key="val_db"
                )
                schemas = get_schemas(st.session_state.conn, val_db)
                val_source_schema = st.selectbox(
                    "Source Schema",
                    schemas,
                    index=0,
                    key="val_source_schema"
                )
                val_target_schema = st.selectbox(
                    "Target Schema",
                    schemas,
                    index=1 if len(schemas) > 1 else 0,
                    key="val_target_schema"
                )
                
                if st.button("Run Validation", type="primary"):
                    with st.spinner("Running validation..."):
                        try:
                            # Compare tables
                            table_diff = compare_table_differences(
                                st.session_state.conn, val_db, val_source_schema, val_target_schema
                            )
                            
                            # Compare columns and data types
                            column_diff, datatype_diff = compare_column_differences(
                                st.session_state.conn, val_db, val_source_schema, val_target_schema
                            )
                            
                            st.success("✅ Validation completed successfully!")
                            
                            tab1, tab2, tab3 = st.tabs([
                                "Table Differences", 
                                "Column Differences", 
                                "Data Type Differences"
                            ])
                            
                            with tab1:
                                st.dataframe(table_diff)
                            
                            with tab2:
                                st.dataframe(column_diff)
                            
                            with tab3:
                                st.dataframe(datatype_diff)
                            
                        except Exception as e:
                            st.error(f"❌ Validation failed: {str(e)}")
            
            with col2:
                st.markdown("### Validation Results")
                st.info("Configure validation on the left and click 'Run Validation'")
        
        # ===== KPI VALIDATION TAB =====
        with tab3:
            st.subheader("KPI Validation")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### KPI Configuration")
                kpi_db = st.selectbox(
                    "Database",
                    get_databases(st.session_state.conn),
                    index=0,
                    key="kpi_db"
                )
                schemas = get_schemas(st.session_state.conn, kpi_db)
                kpi_source_schema = st.selectbox(
                    "Source Schema",
                    schemas,
                    index=0,
                    key="kpi_source_schema"
                )
                kpi_target_schema = st.selectbox(
                    "Target Schema",
                    schemas,
                    index=1 if len(schemas) > 1 else 0,
                    key="kpi_target_schema"
                )
                
                if st.button("Run KPI Validation", type="primary"):
                    with st.spinner("Running KPI validation..."):
                        df, message = validate_kpis(
                            st.session_state.conn, kpi_db, kpi_source_schema, kpi_target_schema
                        )
                        
                        if not df.empty:
                            st.success(message)
                            st.dataframe(df)
                        else:
                            st.error(message)
            
            with col2:
                st.markdown("### KPI Results")
                st.info("Configure KPI validation on the left and click 'Run KPI Validation'")

if __name__ == "__main__":
    main()
