import io
import base64
import pandas as pd
import numpy as np
import dask.dataframe as dd
from typing import Dict, Any

# Delay matplotlib import ensuring non-GUI backend
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

def get_base64_image(fig: plt.Figure, max_width_px: int = 800) -> str:
    buf = io.BytesIO()
    base_dpi = 90
    width_in = fig.get_size_inches()[0]
    current_width = max(1, int(width_in * base_dpi))
    target_dpi = base_dpi
    if current_width > max_width_px:
        target_dpi = max(55, int(base_dpi * (max_width_px / current_width)))

    fig.savefig(
        buf,
        format='png',
        bbox_inches='tight',
        dpi=target_dpi,
        pil_kwargs={"optimize": True},
    )
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return f"data:image/png;base64,{img_str}"

def generate_visualizations(df: Any, json_summary: Dict[str, Any]) -> Dict[str, str]:
    """
    Generate plots as base64 strings.
    Limits to 50k rows sample for safety.
    Theme: #1a1a2e base, #e94560 accent
    """
    plots = {}
    is_dask = isinstance(df, dd.DataFrame)
    
    # Sampling
    if is_dask:
        frac = min(1.0, 50000 / len(df)) if len(df) > 0 else 1.0
        sample_df = df.sample(frac=frac, random_state=42).compute()
    else:
        sample_df = df.sample(n=min(50000, len(df)), random_state=42) if len(df) > 50000 else df
        
    sampled_label = "Sampled: 50k rows" if len(df) > 50000 else "All records"

    sns.set_theme(style="darkgrid", rc={"axes.facecolor": "#1a1a2e", "figure.facecolor": "#1a1a2e", "text.color": "white", "axes.labelcolor": "white", "xtick.color": "white", "ytick.color": "white"})
    accent_color = "#e94560"

    # 1. Missing Values Heatmap
    if sample_df.isnull().sum().sum() > 0:
        fig, ax = plt.subplots(figsize=(8, 3.6))
        sns.heatmap(sample_df.isnull(), cbar=False, cmap="crest", ax=ax, yticklabels=False)
        ax.set_title(f"Missing Values Matrix ({sampled_label})", color="white")
        plots['missing_heatmap'] = get_base64_image(fig)

    # 2. Correlation Heatmap
    numeric_cols = [c['name'] for c in json_summary['columns'] if c['distribution_type'] not in ['unknown', 'text', 'categorical'] and c['dtype'] != 'bool']
    if len(numeric_cols) > 1:
        corr = sample_df[numeric_cols].corr()
        fig, ax = plt.subplots(figsize=(7.2, 5.2))
        sns.heatmap(corr, annot=False, cmap="coolwarm", center=0, ax=ax)
        ax.set_title("Correlation Heatmap", color="white")
        plots['correlation_heatmap'] = get_base64_image(fig)

    # 3. Distributions Grid (Numeric)
    if numeric_cols:
        n_cols = len(numeric_cols)
        fig_rows = (n_cols + 3) // 4
        fig, axes = plt.subplots(fig_rows, 4, figsize=(12, 3.2 * fig_rows))
        axes = np.array(axes).flatten()
        
        for i, col in enumerate(numeric_cols):
            sns.histplot(sample_df[col].dropna(), kde=True, color=accent_color, ax=axes[i], bins=30)
            axes[i].set_title(col[:20])
            
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)
            
        fig.tight_layout()
        plots['numeric_distributions'] = get_base64_image(fig)

    # 4. Top-N Categorical
    cat_cols = [c['name'] for c in json_summary['columns'] if c['distribution_type'] == 'categorical' and c['unique_count'] <= 30]
    if cat_cols:
        fig, axes = plt.subplots((len(cat_cols) + 1) // 2, 2, figsize=(11, 3.4 * ((len(cat_cols) + 1) // 2)))
        axes = np.array(axes).flatten()
        for i, col in enumerate(cat_cols):
            v_counts = sample_df[col].value_counts().head(10)
            sns.barplot(x=v_counts.values, y=v_counts.index.astype(str), color=accent_color, ax=axes[i])
            axes[i].set_title(col)
        for j in range(len(cat_cols), len(axes)):
            axes[j].set_visible(False)
        fig.tight_layout()
        plots['categorical_distributions'] = get_base64_image(fig)

    return plots
