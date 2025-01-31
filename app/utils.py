def validate_required_columns(required_columns):
    """
    Validates that required columns are properly defined.
    :param required_columns: List of required columns
    :return: True if valid, False otherwise
    """
    if not required_columns or any(not col.strip() for col in required_columns):
        return False
    return True


def display_table(data):
    """
    Displays a table in the Streamlit app.
    :param data: List of dictionaries representing rows
    """
    import pandas as pd
    df = pd.DataFrame(data)
    st.dataframe(df)