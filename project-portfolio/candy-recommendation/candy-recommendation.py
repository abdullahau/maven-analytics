import marimo

__generated_with = "0.18.4"
app = marimo.App(layout_file="layouts/candy-recommendation.grid.json")

with app.setup(hide_code=True):
    import marimo as mo
    import altair as alt
    import polars as pl
    import numpy as np
    from pathlib import Path
    from sklearn.decomposition import PCA
    from sklearn.cluster import KMeans


@app.cell(hide_code=True)
def _():
    mo.center(mo.md("# Halloween Candy Challenge"))
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **Goal**: Use data to find the 3 types of Halloween candy that will make you the most popular house on the block.

    **Detail**: Using online votes ranking 85 types of candy, your task is to find the 3 treats you'll give our on Halloween to guarantee that trick-or-treaters of ***all tastes*** find something they'll love and present the data to back up your decision.

    **Approach**
    1. View the data
    2. Apply PCA and jitter to prepare the data for a scatter plot
    3. Explore the scatter plot and make recommendations
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 1. View the data
    """)
    return


@app.cell(hide_code=True)
def _():
    path = Path(
        "project-portfolio/candy-recommendation/halloween-candy-rankings"
    )
    pl.read_csv(path / "candy_data_dictionary.csv")
    return (path,)


@app.cell
def _(path):
    candy_data = pl.read_csv(path / "candy-data.csv")
    candy_data
    return (candy_data,)


@app.cell
def _(candy_data):
    candy_subset = candy_data.select(pl.nth(range(1, 10)))
    candy_subset
    return (candy_subset,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 2. Apply Principal Component Analysis (PCA)
    """)
    return


@app.cell
def _(candy_subset):
    pca = PCA(n_components=2)
    pca.fit(candy_subset)
    pca.explained_variance_ratio_
    return (pca,)


@app.cell
def _(candy_subset, pca):
    pca.set_output(transform="polars")
    pca_df = pca.transform(candy_subset)
    pca0_std = pca_df.select("pca0").std().item()
    pca1_std = pca_df.select("pca1").std().item()
    return pca0_std, pca1_std, pca_df


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Add Jitter
    """)
    return


@app.cell
def _(candy_data, pca0_std, pca1_std, pca_df):
    alpha = 0.05

    candy_2d = (
        pca_df
        .with_columns(
            pl.col("pca0") + np.random.normal(0, alpha * pca0_std, size=len(candy_data)),
            pl.col("pca1") + np.random.normal(0, alpha * pca1_std, size=len(candy_data))
            )
    )
    return (candy_2d,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### *k*-means Clustering
    """)
    return


@app.cell
def _(pca_df):
    kmeans = KMeans(n_clusters=4)
    clusters = kmeans.fit_predict(pca_df)
    return (clusters,)


@app.cell
def _():
    range_slider = mo.ui.range_slider(start=0, stop=100, step=1, value=[0, 100],full_width=True)
    return (range_slider,)


@app.cell
def _(range_slider):
    mo.vstack([range_slider, mo.md(f"Win % Range: {range_slider.value}")])
    return


@app.cell
def _(candy_2d, candy_data, clusters, range_slider):
    candy = (
        pl.concat(
            [candy_data, candy_2d, pl.DataFrame({"cluster": clusters})],
            how="horizontal"
        ).filter(
            pl.col("winpercent") >= range_slider.value[0],
            pl.col("winpercent") <= range_slider.value[1]
        )
    )
    candy
    return (candy,)


@app.cell
def _(candy):
    points = (
        alt.Chart(candy)
        .mark_circle(size=95)
        .encode(
            y=alt.Y("pca0:Q"),
            x=alt.X("pca1:Q"),
            color=alt.Color("cluster:N", legend=None),
            fillOpacity="winpercent:Q"
        ).properties(height=750, width=1100)
    )

    text = points.mark_text(
        align='left',
        baseline='middle',
        dx=7
    ).encode(
        text='competitorname'
    )

    mo.ui.altair_chart(points + text)
    return


if __name__ == "__main__":
    app.run()
