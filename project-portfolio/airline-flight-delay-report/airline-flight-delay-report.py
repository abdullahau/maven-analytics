import marimo

__generated_with = "0.18.4"
app = marimo.App()

with app.setup:
    import marimo as mo
    import polars as pl
    import altair as alt

    from pathlib import Path


@app.cell
def _():
    path = Path("project-portfolio/airline-flight-delay-report/airlines-airports-data")
    flights = pl.scan_parquet(path / "flights-selected.parquet")

    flights.collect_schema()
    return


if __name__ == "__main__":
    app.run()
