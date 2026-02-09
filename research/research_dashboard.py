import io
import streamlit as st
import pandas as pd
import plotly.express as px

from compute_statistics import (
    eval_sheet_level_sensitive_data,
    eval_personal_sensitive_data_column_level,
    eval_file_level_sensitive_any_sheets,
    compute_cost_for_model,
)

# -----------------------------------------------------------------------------
# Page config
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title='LLM Sensitive Data Detection Dashboard',
    layout='wide',
)

# -----------------------------------------------------------------------------
# Custom CSS
# -----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
        color: #111;
    }

    h1, h2, h3, h4 {
        color: #009EDB;
        font-weight: 600;
    }

    div.stButton > button,
    div.stDownloadButton > button,
    div.stSelectbox > div {
        background-color: #009EDB !important;
        color: white !important;
        font-weight: 600 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
st.title('🔐 LLM Sensitive Data Detection Dashboard')
st.write('Visualizing performance metrics of LLMs in detecting sensitive data.')

# -----------------------------------------------------------------------------
# Models evaluated
# -----------------------------------------------------------------------------
models = ['gpt-4.1-nano', 'DeepSeek-V3.1']

# -----------------------------------------------------------------------------
# Containers for metrics & errors
# -----------------------------------------------------------------------------
sheet_metrics = []
column_metrics = []
file_metrics = []

sheet_errors = []
column_errors = []
file_errors = []

# -----------------------------------------------------------------------------
# Evaluation
# -----------------------------------------------------------------------------
sheet_categories = ['non_personal_data_sensitive', 'personal_data_sensitive']

for model in models:
    # -------------------------
    # Sheet-level
    # -------------------------
    for cat in sheet_categories:
        try:
            metrics, errors = eval_sheet_level_sensitive_data(model, cat)
            metrics.update(
                {
                    'model': model,
                    'category': f'sheet: {cat}',
                    'level': 'sheet',
                }
            )
            sheet_metrics.append(metrics)

            for e in errors:
                e.setdefault('model', model)
                e.setdefault('category', cat)
            sheet_errors.extend(errors)

        except Exception as e:
            st.error(f'Sheet-level eval failed for {model} / {cat}: {e}')

    # -------------------------
    # Column-level
    # -------------------------
    try:
        metrics, errors = eval_personal_sensitive_data_column_level(model)
        metrics.update(
            {
                'model': model,
                'category': 'column: personal_data_sensitive',
                'level': 'column',
            }
        )
        column_metrics.append(metrics)

        for e in errors:
            e.setdefault('model', model)
        column_errors.extend(errors)

    except Exception as e:
        st.error(f'Column-level eval failed for {model}: {e}')

    # -------------------------
    # File-level
    # -------------------------
    try:
        metrics, errors = eval_file_level_sensitive_any_sheets(model)
        metrics.update(
            {
                'model': model,
                'category': 'file_sensitive',
                'level': 'file',
            }
        )
        file_metrics.append(metrics)

        for e in errors:
            e.setdefault('model', model)
        file_errors.extend(errors)

    except Exception as e:
        st.error(f'File-level eval failed for {model}: {e}')

# -----------------------------------------------------------------------------
# Build DataFrames
# -----------------------------------------------------------------------------
sheet_df = pd.DataFrame(sheet_metrics)
column_df = pd.DataFrame(column_metrics)
file_df = pd.DataFrame(file_metrics)

errors_sheet_df = pd.DataFrame(sheet_errors)
errors_column_df = pd.DataFrame(column_errors)
errors_file_df = pd.DataFrame(file_errors)

# Round metric columns
for df in [sheet_df, column_df, file_df]:
    if not df.empty:
        for c in ['accuracy', 'precision', 'recall', 'f1']:
            if c in df.columns:
                df[c] = df[c].round(2)

# -----------------------------------------------------------------------------
# Metrics overview
# -----------------------------------------------------------------------------
st.subheader('📊 Metrics Overview')

metrics_df = pd.concat([sheet_df, column_df, file_df], ignore_index=True)
st.dataframe(metrics_df)

# -----------------------------------------------------------------------------
# Metric comparison plot
# -----------------------------------------------------------------------------
st.subheader('📈 Metric Comparison Across Models')

metric_choice = st.selectbox(
    'Choose a metric',
    ['accuracy', 'precision', 'recall', 'f1'],
)

fig = px.bar(
    metrics_df,
    x='model',
    y=metric_choice,
    color='category',
    barmode='group',
    text_auto=True,
    range_y=[0, 1],
)
st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------
# File-level metrics (per model)
# -----------------------------------------------------------------------------
st.subheader('📂 File-Level Metrics (Per Model)')

if not file_df.empty:
    fig = px.bar(
        file_df.melt(
            id_vars=['model', 'category'],
            value_vars=['accuracy', 'precision', 'recall', 'f1'],
            var_name='metric',
            value_name='value',
        ),
        x='metric',
        y='value',
        color='model',
        barmode='group',
        text='value',
        range_y=[0, 1],
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info('No file-level metrics available.')

# -----------------------------------------------------------------------------
# Misclassifications – File level
# -----------------------------------------------------------------------------
st.subheader('🚨 File-Level Misclassifications')

if not errors_file_df.empty:
    for model in models:
        st.markdown(f'### {model}')
        model_df = errors_file_df[errors_file_df['model'] == model]
        if model_df.empty:
            st.success('No file-level misclassifications 🎉')
        else:
            st.dataframe(model_df)

    csv_buf = io.StringIO()
    errors_file_df.to_csv(csv_buf, index=False)
    st.download_button(
        'Download file-level misclassifications (CSV)',
        csv_buf.getvalue(),
        'file_level_misclassifications.csv',
        'text/csv',
    )
else:
    st.success('🎉 No file-level misclassifications detected!')

# -----------------------------------------------------------------------------
# Misclassifications – Column level
# -----------------------------------------------------------------------------
st.subheader('⚠️ Column-Level Misclassifications')

if not errors_column_df.empty:
    for model in models:
        st.markdown(f'### {model}')
        model_df = errors_column_df[errors_column_df['model'] == model]
        if model_df.empty:
            st.success('No column-level misclassifications 🎉')
        else:
            st.dataframe(model_df)

    csv_buf = io.StringIO()
    errors_column_df.to_csv(csv_buf, index=False)
    st.download_button(
        'Download column-level misclassifications (CSV)',
        csv_buf.getvalue(),
        'column_level_misclassifications.csv',
        'text/csv',
    )
else:
    st.success('🎉 No column-level misclassifications detected!')

# -----------------------------------------------------------------------------
# Misclassifications – Sheet level
# -----------------------------------------------------------------------------
st.subheader('📄 Sheet-Level Misclassifications')

if not errors_sheet_df.empty:
    for model in models:
        st.markdown(f'### {model}')
        model_df = errors_sheet_df[errors_sheet_df['model'] == model]
        if model_df.empty:
            st.success('No sheet-level misclassifications 🎉')
        else:
            st.dataframe(model_df)

    csv_buf = io.StringIO()
    errors_sheet_df.to_csv(csv_buf, index=False)
    st.download_button(
        'Download sheet-level misclassifications (CSV)',
        csv_buf.getvalue(),
        'sheet_level_misclassifications.csv',
        'text/csv',
    )
else:
    st.success('🎉 No sheet-level misclassifications detected!')

# -----------------------------------------------------------------------------
# Cost analysis
# -----------------------------------------------------------------------------
st.subheader('💰 Token Usage & Cost')

cost_rows = []
for model in models:
    try:
        cost_rows.append(compute_cost_for_model(model))
    except Exception as e:
        st.error(f'Cost computation failed for {model}: {e}')

cost_df = pd.DataFrame(cost_rows)
st.dataframe(cost_df)

fig = px.bar(
    cost_df,
    x='model',
    y='total_cost_usd',
    text_auto=True,
    title='Total Cost per Model (USD)',
)
st.plotly_chart(fig, use_container_width=True)
