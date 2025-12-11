import io
import streamlit as st
import pandas as pd
import plotly.express as px
from compute_statistics import (
    eval_sheet_level_sensitive_data,
    eval_personal_sensitive_data_column_level,
    compute_cost_for_model,
)

# Page Configuration
st.set_page_config(page_title='LLM Sensitive Data Detection Dashboard', layout='wide')

# --- Custom CSS for sleek styling ---
st.markdown(
    """
    <style>
    /* Import Outfit font from Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap');

    /* Apply Outfit to the whole app */
    html, body, [class*="css"]  {
        font-family: 'Outfit', sans-serif;
        color: #111;
    }

    /* Style main title */
    .css-1v3fvcr h1 {
        color: #009EDB;
        font-weight: 700;
    }

    /* Style subheaders */
    h2, h3, h4 {
        color: #009EDB;
        font-weight: 600;
    }


    /* Style selectbox, buttons, and download buttons */
    div.stButton > button, div.stDownloadButton > button, div.stSelectbox > div {
        background-color: #009EDB !important;
        color: white !important;
        font-weight: 600 !important;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


st.title('🔐 LLM Sensitive Data Detection Dashboard')
st.write('Visualizing performance metrics of LLMs in detecting sensitive data.')

# LLMs evaluated
models = ['gpt-4.1-nano', 'gpt-4.1-mini']

# Collect statistics:
data = []
all_errors = []  # collect misclassification rows across models

# Sheet-level categories
sheet_categories = ['non_pii_sensitive', 'pii_sensitive']

for model in models:
    # Sheet-level stats
    for cat in sheet_categories:
        try:
            metrics = eval_sheet_level_sensitive_data(model, cat)
        except AssertionError as e:
            st.error(f'File mismatch for model {model} / category {cat}: {e}')
            metrics = {'accuracy': 0, 'precision': 0, 'recall': 0, 'f1': 0}
        except Exception as e:
            st.error(f'Error computing sheet-level metrics for {model}, {cat}: {e}')
            metrics = {'accuracy': 0, 'precision': 0, 'recall': 0, 'f1': 0}

        metrics['model'] = model
        metrics['category'] = f'sheet: {cat}'
        data.append(metrics)

    # Column-level stats — returns (metrics_dict, errors_list)
    try:
        column_stats, column_errors = eval_personal_sensitive_data_column_level(model)
    except AssertionError as e:
        st.error(f'File mismatch for column-level evaluation for model {model}: {e}')
        column_stats = {'accuracy': 0, 'precision': 0, 'recall': 0, 'f1': 0}
        column_errors = []
    except Exception as e:
        st.error(f'Error computing column-level metrics for {model}: {e}')
        column_stats = {'accuracy': 0, 'precision': 0, 'recall': 0, 'f1': 0}
        column_errors = []

    column_stats['model'] = model
    column_stats['category'] = 'column: pii_sensitive'
    data.append(column_stats)

    # extend global errors (already obtained once per model)
    if column_errors:
        # ensure each error row has model info
        for err in column_errors:
            if 'model' not in err:
                err['model'] = model
        all_errors.extend(column_errors)

# Build metrics DataFrame
df = pd.DataFrame(data)

# Layout with columns
col1, col2 = st.columns(2)
with col1:
    st.subheader('📊 Raw Metrics Data (All Categories)')
    if not df.empty:
        st.dataframe(df)
    else:
        st.info('No metrics available yet.')

with col2:
    st.subheader('🧩 Column-Level Only Metrics')
    col_df = df[df['category'] == 'column: pii_sensitive']
    if not col_df.empty:
        st.dataframe(col_df)
    else:
        st.info('No column-level metrics available yet.')

# Bar chart visualization
st.subheader('📈 Metrics Comparison Across Models')
metric = st.selectbox('Choose a metric to visualize', ['accuracy', 'precision', 'recall', 'f1'])

if not df.empty:
    fig = px.bar(
        df,
        x='model',
        y=metric,
        color='category',
        barmode='group',
        title=f'{metric.capitalize()} Comparison',
        text_auto=True,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info('Metrics will appear here once computed.')

# Misclassified Columns Report
st.subheader('⚠️ Misclassified Columns Report')
errors_df = pd.DataFrame(all_errors)

if not errors_df.empty:
    st.dataframe(errors_df)

    # Provide CSV download for errors
    csv_buf = io.StringIO()
    errors_df.to_csv(csv_buf, index=False)
    csv_bytes = csv_buf.getvalue().encode('utf-8')
    st.download_button(
        label='Download misclassification report (CSV)',
        data=csv_bytes,
        file_name='misclassified_columns.csv',
        mime='text/csv',
    )
else:
    st.success('🎉 No column-level misclassifications detected!')

# Token Usage & Cost Efficiency
st.subheader('💰 Token Usage & Cost Efficiency')

cost_data = []
for m in models:
    try:
        cost_data.append(compute_cost_for_model(m))
    except Exception as e:
        st.error(f'Error computing cost for model {m}: {e}')
        cost_data.append({'model': m, 'total_prompt_tokens': 0, 'total_completion_tokens': 0, 'total_cost_usd': 0.0})

cost_df = pd.DataFrame(cost_data)
if not cost_df.empty:
    st.dataframe(cost_df)

    fig_cost = px.bar(
        cost_df,
        x='model',
        y='total_cost_usd',
        text_auto=True,
        title='Total PII Detection Cost per Model (USD)',
    )
    st.plotly_chart(fig_cost, use_container_width=True)
else:
    st.info('No cost data available.')

# Combine performance and cost: cost vs F1 scatter (efficiency)
st.subheader('📉 Cost vs Performance Efficiency')

# prepare a small table linking model -> f1 (we use column-level f1 if present, otherwise sheet-level pii)
perf_rows = []
for model in models:
    # prefer column-level F1
    row = df[(df['model'] == model) & (df['category'] == 'column: pii_sensitive')]
    if not row.empty:
        f1_val = float(row.iloc[0]['f1'])
    else:
        # fallback to sheet: pii_sensitive
        row2 = df[(df['model'] == model) & (df['category'] == 'sheet: pii_sensitive')]
        f1_val = float(row2.iloc[0]['f1']) if not row2.empty else 0.0

    cost_row = cost_df[cost_df['model'] == model]
    cost_val = float(cost_row.iloc[0]['total_cost_usd']) if not cost_row.empty else 0.0

    perf_rows.append(
        {
            'model': model,
            'f1': f1_val,
            'total_cost_usd': cost_val,
            'efficiency': (f1_val / cost_val) if cost_val > 0 else None,
        }
    )

perf_df = pd.DataFrame(perf_rows)
if not perf_df.empty:
    st.dataframe(perf_df)
    fig_scatter = px.scatter(
        perf_df,
        x='total_cost_usd',
        y='f1',
        hover_data=['model', 'efficiency'],
        text='model',
        title='Cost vs F1 (higher is better)',
    )
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.info('No performance vs cost data to show.')
