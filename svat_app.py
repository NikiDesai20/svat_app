import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder

# --- Helper Functions ---
def get_snowflake_connection(user, password, account, warehouse=None, database=None, schema=None):
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
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

def execute_kpi_query(cursor, query, schema):
    """Execute a KPI query against a specific schema"""
    try:
        # Replace the placeholder table name in the KPI query with fully qualified schema.table
        executed_query = query.replace('ORDER_DATA', f'{schema}.ORDER_DATA')
        cursor.execute(executed_query)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        return f"QUERY_ERROR: {str(e)}"

def validate_kpis(conn, database, source_schema, target_schema):
    """Validate KPIs between source and clone schemas with improved logic"""
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
                result_source = execute_kpi_query(cursor, kpi_query, source_schema)
                
                # Execute against target schema
                result_clone = execute_kpi_query(cursor, kpi_query, target_schema)

                # Calculate differences if both results are numeric
                diff = None
                pct_diff = None
                status = '⚠️ Mismatch'
                
                if isinstance(result_source, str) and result_source.startswith('QUERY_ERROR'):
                    status = '❌ Source Error'
                elif isinstance(result_clone, str) and result_clone.startswith('QUERY_ERROR'):
                    status = '❌ Clone Error'
                elif result_source == result_clone:
                    status = '✅ Match'
                else:
                    try:
                        # Try numeric comparison
                        num_source = float(result_source)
                        num_clone = float(result_clone)
                        diff = num_source - num_clone
                        if num_source != 0:
                            pct_diff = (diff / num_source) * 100
                        else:
                            pct_diff = float('inf')
                    except (ValueError, TypeError):
                        # Non-numeric comparison
                        diff = "N/A"
                        pct_diff = "N/A"

                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Query': kpi_query[:100] + '...' if len(kpi_query) > 100 else kpi_query,
                    'Source Value': result_source,
                    'Clone Value': result_clone,
                    'Difference': diff if diff is not None else "N/A",
                    'Difference %': f"{pct_diff:.2f}%" if pct_diff is not None else "N/A",
                    'Status': status
                })

            except Exception as e:
                results.append({
                    'KPI ID': kpi_id,
                    'KPI Name': kpi_name,
                    'Query': kpi_query[:100] + '...' if len(kpi_query) > 100 else kpi_query,
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

# --- Streamlit UI ---
st.set_page_config(
    page_title="Snowflake Validation Automation Tool",
    page_icon="❄️",
    layout="wide"
)

# Add custom CSS
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
    }
    .stTextInput>div>div>input, .stTextInput>div>div>input:focus {
        border: 1px solid #4a90e2;
    }
    .stSelectbox>div>div>select {
        border: 1px solid #4a90e2;
    }
    .css-1aumxhk {
        background-color: #f0f2f6;
        border-radius: 0.5rem;
        padding: 1rem;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: bold;
        color: #4a90e2;
    }
</style>
""", unsafe_allow_html=True)

# Session state
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'current_db' not in st.session_state:
    st.session_state.current_db = None

# --- LOGIN SECTION ---
st.sidebar.title("Snowflake Connection")
with st.sidebar.form("login_form"):
    user = st.text_input("Username", value="SJSNOWFLAKE")
    password = st.text_input("Password", type="password", value="SagarCloudlabs123")
    account = st.text_input("Account", value="RIJUPCW-AE85803")
    warehouse = st.text_input("Warehouse", value="PROCESS_WH")
    
    login_col, disconnect_col = st.columns(2)
    with login_col:
        login_btn = st.form_submit_button("Connect", type="primary")
    with disconnect_col:
        disconnect_btn = st.form_submit_button("Disconnect")

if login_btn:
    with st.spinner("Connecting to Snowflake..."):
        st.session_state.conn, msg = get_snowflake_connection(
            user, password, account, warehouse
        )
        if st.session_state.conn:
            st.sidebar.success(msg)
            st.session_state.databases = get_databases(st.session_state.conn)
        else:
            st.sidebar.error(msg)

if disconnect_btn and st.session_state.conn:
    st.session_state.conn, msg = disconnect_snowflake(st.session_state.conn)
    st.sidebar.info(msg)
    st.session_state.conn = None
    st.session_state.current_db = None
    st.experimental_rerun()

# --- MAIN CONTENT ---
st.title("❄️ Snowflake Validation Automation Tool")

if st.session_state.conn:
    # Show connection info
    with st.sidebar.expander("Connection Info"):
        st.json({
            "user": user,
            "account": account,
            "warehouse": warehouse,
            "connected": st.session_state.conn is not None
        })
    
    # --- CLONE SECTION ---
    with st.expander("⎘ Schema Clone", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            source_db = st.selectbox(
                "Source Database",
                options=st.session_state.databases,
                key="clone_source_db"
            )
            st.session_state.clone_schemas = get_schemas(st.session_state.conn, source_db)
            source_schema = st.selectbox(
                "Source Schema",
                options=st.session_state.clone_schemas,
                key="clone_source_schema"
            )
            target_schema = st.text_input(
                "Target Schema Name",
                value=f"{source_schema}_CLONE",
                key="clone_target_schema"
            )
            
            if st.button("Execute Clone"):
                with st.spinner(f"Cloning {source_db}.{source_schema} to {target_schema}..."):
                    success, message, df = clone_schema(
                        st.session_state.conn, 
                        source_db, 
                        source_schema, 
                        target_schema
                    )
                    
                    if success:
                        st.success(message)
                        st.dataframe(df)
                    else:
                        st.error(message)
    
    # --- VALIDATION SECTION ---
    with st.expander("🔍 Schema Validation"):
        tab1, tab2, tab3 = st.tabs(["Table Differences", "Column Differences", "Data Type Differences"])
        
        val_col1, val_col2 = st.columns(2)
        with val_col1:
            val_db = st.selectbox(
                "Database",
                options=st.session_state.databases,
                key="val_db"
            )
            st.session_state.val_schemas = get_schemas(st.session_state.conn, val_db)
        
        with val_col2:
            val_source_schema = st.selectbox(
                "Source Schema",
                options=st.session_state.val_schemas,
                key="val_source_schema"
            )
            val_target_schema = st.selectbox(
                "Target Schema",
                options=st.session_state.val_schemas,
                key="val_target_schema"
            )
        
        if st.button("Run Validation"):
            with st.spinner("Running validation..."):
                # Table differences
                with tab1:
                    table_diff = compare_table_differences(
                        st.session_state.conn, 
                        val_db, 
                        val_source_schema, 
                        val_target_schema
                    )
                    if not table_diff.empty:
                        st.dataframe(table_diff)
                    else:
                        st.info("No table differences found")
                
                # Column differences
                with tab2:
                    column_diff, _ = compare_column_differences(
                        st.session_state.conn, 
                        val_db, 
                        val_source_schema, 
                        val_target_schema
                    )
                    if not column_diff.empty:
                        st.dataframe(column_diff)
                    else:
                        st.info("No column differences found")
                
                # Data type differences
                with tab3:
                    _, datatype_diff = compare_column_differences(
                        st.session_state.conn, 
                        val_db, 
                        val_source_schema, 
                        val_target_schema
                    )
                    if not datatype_diff.empty:
                        st.dataframe(datatype_diff)
                    else:
                        st.info("No data type differences found")
    
    # --- KPI VALIDATION SECTION ---
    with st.expander("📊 KPI Validation"):
        kpi_col1, kpi_col2 = st.columns(2)
        
        with kpi_col1:
            kpi_db = st.selectbox(
                "Database",
                options=st.session_state.databases,
                key="kpi_db"
            )
            st.session_state.kpi_schemas = get_schemas(st.session_state.conn, kpi_db)
        
        with kpi_col2:
            kpi_source_schema = st.selectbox(
                "Source Schema",
                options=st.session_state.kpi_schemas,
                key="kpi_source_schema"
            )
            kpi_target_schema = st.selectbox(
                "Target Schema",
                options=st.session_state.kpi_schemas,
                key="kpi_target_schema"
            )
        
        if st.button("Run KPI Validation", key="run_kpi"):
            with st.spinner("Validating KPIs..."):
                kpi_results, status = validate_kpis(
                    st.session_state.conn,
                    kpi_db,
                    kpi_source_schema,
                    kpi_target_schema
                )
                
                if not kpi_results.empty:
                    # Configure AgGrid for better display
                    gb = GridOptionsBuilder.from_dataframe(kpi_results)
                    gb.configure_column("Query", width=300)
                    gb.configure_column("Status", width=150, cellStyle={
                        'styleConditions': [
                            {'condition': "params.value.includes('✅')", 'style': {'color': 'green'}},
                            {'condition': "params.value.includes('⚠️')", 'style': {'color': 'orange'}},
                            {'condition': "params.value.includes('❌')", 'style': {'color': 'red'}}
                        ]
                    })
                    gridOptions = gb.build()
                    
                    AgGrid(
                        kpi_results,
                        gridOptions=gridOptions,
                        height=400,
                        width='100%',
                        theme='streamlit',
                        fit_columns_on_grid_load=True
                    )
                    
                    # Show summary metrics
                    total_kpis = len(kpi_results)
                    matched_kpis = len(kpi_results[kpi_results['Status'] == '✅ Match'])
                    error_kpis = len(kpi_results[kpi_results['Status'].str.contains('❌')])
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total KPIs", total_kpis)
                    with col2:
                        st.metric("Matched KPIs", f"{matched_kpis} ({matched_kpis/total_kpis:.0%})")
                    with col3:
                        st.metric("Errors", error_kpis, delta_color="inverse")
                else:
                    st.warning(status)
else:
    st.warning("Please connect to Snowflake using the sidebar")
