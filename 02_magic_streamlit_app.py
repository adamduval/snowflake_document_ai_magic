import time
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Set up the Streamlit page configuration
st.set_page_config(layout='wide')

# App title and description
st.title("✨ Magic with Snowflake Document AI ✨")
st.write(
    """
    A proof of concept Streamlit app to capture Document AI information.
    """
)

# Get the current Snowflake session
session = get_active_session()

def load_form_table():
    """
    Load and format data from the 'form_table'.
    
    Returns:
        pd.DataFrame: A pandas DataFrame with selected columns.
    """
    data = session.table("form_table").to_pandas()
    # Select specific columns (1, 4, 2, 3, 5)
    return data.iloc[:, [1, 4, 2, 3, 5]]

# Initialize DataFrames
df = load_form_table()
prev_df = None

# Placeholder for dynamic updates
placeholder = st.empty()

# Monitor for changes and update the display
for _ in range(1440):  # 1440 iterations (e.g., check for updates every minute for 24 hours)
    df = load_form_table()

    if prev_df is not None and not df.equals(prev_df):
        # Trigger a visual effect if data has changed
        st.snow()

    prev_df = df.copy()

    # Update the displayed DataFrame in the placeholder
    with placeholder.container():
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Sleep for 1 second before the next iteration
    time.sleep(1)
