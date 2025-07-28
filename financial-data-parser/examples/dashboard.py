"""
Financial Data Parser Dashboard
Streamlit-based UI for exploring and analyzing financial data
"""

import streamlit as st
import pandas as pd
import time
import plotly.express as px
from pathlib import Path
import sys
import threading
from decimal import Decimal

# Add src directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import your core modules
from src.core.excel_processor import ExcelProcessor
from src.core.type_detector import TypeDetector
from src.core.format_parser import FormatParser
from src.core.data_storage import DataStorage

# Dashboard setup
st.set_page_config(
    page_title="Financial Data Parser",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if "processor" not in st.session_state:
    st.session_state.processor = ExcelProcessor()
if "storage" not in st.session_state:
    st.session_state.storage = DataStorage()
if "main_thread_id" not in st.session_state:
    st.session_state.main_thread_id = threading.get_ident()

# Store uploaded files in session state
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = None

# Check for thread changes
current_thread_id = threading.get_ident()
if current_thread_id != st.session_state.main_thread_id:
    # Preserve uploaded files
    existing_files = st.session_state.uploaded_files
    
    # Reset storage and processor
    st.session_state.storage = DataStorage()
    st.session_state.processor = ExcelProcessor()
    st.session_state.main_thread_id = current_thread_id
    st.session_state.uploaded_files = existing_files
    
    # Re-process files if they exist
    if st.session_state.uploaded_files:
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        file_paths = []
        
        for file in st.session_state.uploaded_files:
            path = temp_dir / file.name
            with open(path, "wb") as f:
                f.write(file.getbuffer())
            file_paths.append(str(path))
        
        with st.spinner("Re-processing files for new thread..."):
            st.session_state.processor.load_files(file_paths)
            st.session_state.info = st.session_state.processor.get_sheet_info()

# Dashboard header
st.title("📊 Financial Data Parser Dashboard")
st.markdown("""
**Explore, analyze, and query financial data with AI-powered parsing**
""")

# Sidebar configuration
with st.sidebar:
    st.header("Configuration")
    uploaded_files = st.file_uploader(
        "Upload Excel Files", 
        type=["xlsx", "xls"],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        # Save to session state
        st.session_state.uploaded_files = uploaded_files
        
        # Save uploaded files to temp directory
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        file_paths = []
        
        for file in uploaded_files:
            path = temp_dir / file.name
            with open(path, "wb") as f:
                f.write(file.getbuffer())
            file_paths.append(str(path))
        
        # Process files
        with st.spinner("Processing files..."):
            st.session_state.processor.load_files(file_paths)
            st.session_state.info = st.session_state.processor.get_sheet_info()
        st.success(f"Processed {len(file_paths)} files!")

# Main dashboard tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📂 Data Explorer", 
    "🔍 Type Detection", 
    "🧮 Format Parsing", 
    "⚡ Query Engine"
])

# Tab 1: Data Explorer
with tab1:
    st.header("Data Explorer")
    
    if not uploaded_files:
        st.info("Upload Excel files to begin analysis")
    else:
        # File and sheet selection
        file_key = st.selectbox(
            "Select File", 
            list(st.session_state.info.keys())
        )
        
        if file_key in st.session_state.info:
            sheet_names = list(st.session_state.info[file_key].keys())
            sheet_name = st.selectbox("Select Sheet", sheet_names)
            
            # Display metadata
            meta = st.session_state.info[file_key][sheet_name]
            col1, col2, col3 = st.columns(3)
            col1.metric("Rows", meta["rows"])
            col2.metric("Columns", meta["cols"])
            col3.metric("Status", "Processed" if "error" not in meta else "Error")
            
            # Data preview
            st.subheader("Data Preview")
            df_preview = st.session_state.processor.preview_data(
                file_key, sheet_name, rows=10
            )
            st.dataframe(df_preview, height=300)

# Tab 2: Type Detection
with tab2:
    st.header("Column Type Detection")
    
    if not uploaded_files:
        st.info("Upload Excel files to detect column types")
    else:
        # Run type detection
        if "detection_results" not in st.session_state:
            with st.spinner("Detecting column types..."):
                df_preview = st.session_state.processor.preview_data(
                    file_key, sheet_name, rows=200
                )
                st.session_state.detection_results = TypeDetector.detect_all(df_preview)
        
        # Visualization
        detection_df = pd.DataFrame.from_dict(
            st.session_state.detection_results, 
            orient="index",
            columns=["Type", "Confidence"]
        ).reset_index().rename(columns={"index": "Column"})
        
        # Show results
        fig = px.bar(
            detection_df,
            x="Column",
            y="Confidence",
            color="Type",
            title="Column Type Confidence Scores",
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.dataframe(detection_df)

# Tab 3: Format Parsing
with tab3:
    st.header("Format Parsing Demo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Amount Parsing")
        amount_input = st.text_input("Enter amount:", "$1,234.56")
        
        if st.button("Parse Amount"):
            try:
                parsed = FormatParser.parse_amount(amount_input)
                st.success(f"Parsed value: {parsed}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    with col2:
        st.subheader("Date Parsing")
        date_input = st.text_input("Enter date:", "31-Dec-2023")
        
        if st.button("Parse Date"):
            try:
                parsed = FormatParser.parse_date(date_input)
                st.success(f"Parsed date: {parsed}")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    st.subheader("Vectorized Parsing Demo")
    if not uploaded_files:
        st.info("Upload files to see vectorized parsing")
    else:
        df = st.session_state.processor.preview_data(file_key, sheet_name, rows=20)
        if "Amount" in df.columns:
            with st.spinner("Parsing amounts..."):
                df["Parsed Amount"] = FormatParser.parse_amount_vectorized(df["Amount"])
                st.dataframe(df[["Amount", "Parsed Amount"]])
        else:
            st.warning("No 'Amount' column found in selected sheet")

# Tab 4: Query Engine
with tab4:
    st.header("Data Query Engine")
    
    if not uploaded_files:
        st.info("Upload files to enable querying")
    else:
        # Always ensure the data is stored before querying
        if "query_df" not in st.session_state:
            # Prepare data
            file_key = list(st.session_state.info.keys())[0]
            sheet_name = list(st.session_state.info[file_key].keys())[0]
            df = st.session_state.processor.preview_data(file_key, sheet_name, rows=500, 
                                                        columns=["Posting Date", "Amount"])
            
            # Clean and parse
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]
            if "amount" in df.columns:
                df["amount_num"] = FormatParser.parse_amount_vectorized(df["amount"])
            if "posting_date" in df.columns:
                df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")
                df = df.dropna(subset=["posting_date"])
            
            st.session_state.query_df = df
        
        # Always store the data
        st.session_state.storage.store(
            "query_data", 
            st.session_state.query_df,
            {},
            index_cols=["posting_date", "amount_num"] if "posting_date" in st.session_state.query_df.columns and "amount_num" in st.session_state.query_df.columns else None
        )
        
        # Query interface
        query_type = st.radio("Query Type", ["Amount Range", "Date Range", "Custom SQL"])
        
        if query_type == "Amount Range":
            min_val = st.number_input("Min Amount", value=0)
            max_val = st.number_input("Max Amount", value=10000)
            
            if st.button("Run Amount Query"):
                min_dec = Decimal(str(min_val))
                max_dec = Decimal(str(max_val))
                
                results = st.session_state.storage.query(
                    "query_data", 
                    amount_range=(min_dec, max_dec)
                )
                st.dataframe(results)
        
        elif query_type == "Date Range":
            start_date = st.date_input("Start Date")
            end_date = st.date_input("End Date")
            
            if st.button("Run Date Query"):
                results = st.session_state.storage.query(
                    "query_data", 
                    date_range=(str(start_date), str(end_date)))
                st.dataframe(results)
        
        else:
            sql_query = st.text_area("SQL Query", "SELECT * FROM query_data LIMIT 10")
            if st.button("Execute SQL"):
                try:
                    results = st.session_state.storage.sql(sql_query)
                    st.dataframe(results)
                except Exception as e:
                    st.error(f"SQL Error: {str(e)}")

# Performance section at bottom
st.divider()
st.header("Performance Metrics")

if "performance" not in st.session_state:
    st.session_state.performance = {}

if st.button("Run Performance Tests"):
    with st.spinner("Running benchmarks..."):
        # Simple performance tests
        start = time.time()
        st.session_state.processor.load_files([file_key])
        st.session_state.performance["load_files"] = time.time() - start
        
        start = time.time()
        st.session_state.processor.get_sheet_info()
        st.session_state.performance["get_sheet_info"] = time.time() - start
        
        if "amount" in st.session_state.query_df.columns:
            start = time.time()
            FormatParser.parse_amount_vectorized(st.session_state.query_df["amount"])
            st.session_state.performance["parse_amount_vectorized"] = time.time() - start

if st.session_state.performance:
    st.subheader("Operation Timings")
    for op, time_taken in st.session_state.performance.items():
        st.metric(op, f"{time_taken:.4f} seconds")