import marimo

__generated_with = "0.18.4"
app = marimo.App(
    width="medium",
    layout_file="layouts/toy-store-kpi-report.grid.json",
)

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import altair as alt
    from pathlib import Path


@app.function(hide_code=True)
@alt.theme.register("marimo_light", enable=True)
def marimo_light():
    return {
        "config": {
            "background": "transparent",
            "view": {"strokeWidth": 0},
            "axis": {
                "grid": False,
                "domain": False,
                "tickColor": "#999",
                "labelColor": "#444",
                "titleColor": "#444",
            },
            "line": {"strokeWidth": 2},
            "area": {"opacity": 0.6},
        }
    }


@app.cell
def _():
    alt.theme.enable("marimo_light")
    return


@app.cell(hide_code=True)
def _():
    mo.center(mo.md(r"# Toy Store KPI Report"))
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The Situation

    You're a brand new Data Analyst for Maven Toys, a toy store chain with multiple store locations in Mexico.

    ### The Assignment

    You have access to data containing transactional records from January 2022 - September 2023, along with information about products and store locations.

    Your goal is to build a simple, interactive report that the leadership team can use to monitor key business metrics and high-level trends.

    ### The Objectives

    1. Connect and profile the data
    2. Create a relational model
    3. Add calculated measures & fields
    4. Build an interactive report
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Objective 1: Connect and Profile the Data
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    1. Connect to the sales, products, stores, and calendar csv files
    2. Review table columns, check for blank or null values, confirm that datatypes are accurately defined and identify any primary and foreign keys
    3. Take a moment to profile the data. How many transactions were recorded? How many stores does Maven Toys operate? What are the lowest and highest priced products?
    4. Add calculated columns in the calendar table for "start of month" and "start of week".
    """)
    return


@app.cell
def _():
    def load_sales(path: Path) -> pl.LazyFrame:
        return pl.scan_parquet(path)

    def load_products(path: Path) -> pl.LazyFrame:
        return pl.scan_csv(path).with_columns(
            pl.col("Product_Cost", "Product_Price")
            .str.replace("$", "", literal=True)
            .str.replace(" ", "", literal=True)
            .cast(pl.Float64)
        )

    def load_stores(path: Path) -> pl.LazyFrame:
        return pl.scan_csv(path, try_parse_dates=True)

    def load_calendar(path: Path) -> pl.LazyFrame:
        return (
            pl.scan_csv(path)
            .with_columns(pl.col("Date").str.to_date(format="%m/%d/%Y"))
            .with_columns(
                Start_Month=pl.col("Date").dt.month_start(),
                Start_Week=(
                    pl.col("Date")
                    .dt.offset_by("1d")  # shift so Sunday becomes Monday
                    .dt.truncate("1w")  # truncate (Monday-based)
                    .dt.offset_by("-1d")  # shift back to Sunday
                ),
            )
        )
    return load_calendar, load_products, load_sales, load_stores


@app.cell
def _(load_calendar, load_products, load_stores):
    data_path = Path("project-portfolio/toy-store-kpi-report/maven-toys-data")
    products = load_products(data_path / "products.csv")
    stores = load_stores(data_path / "stores.csv")
    calendar = load_calendar(data_path / "calendar.csv")
    return calendar, data_path, products, stores


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Objective 2: Create a Relational Model & Add Calculated Measures & Fields
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    1. Load the tables to the data model and create relationships from the sales table to the product, stores, and calendar tables.
    2. Confirm that you are following data modeling best practices. Your model should take the form of a star schema, with 1:Many relationships between fact and dimension tables.
    3. Create a date hierarchy containing the "start of month", "start of week", and "date" fields.
    4. Create calculated columns in the sales table to pull in "cost" and "price" from the products table, then use those fields to calculate the revenue and profit for each transaction.
    5. Create measures to calculate the count of orders ("total orders"), sum of revenue ("total revenue") and sum of profit ("total profit").
    """)
    return


@app.cell
def _(calendar, data_path, load_sales, products, stores):
    sales = (
        load_sales(data_path / "sales.parquet")
        .join(products, on="Product_ID", how="left")
        .join(stores, on="Store_ID", how="left")
        .join(calendar, on="Date", how="left")
        .drop("Store_ID", "Product_ID")
        .with_columns(
            Revenue=pl.col("Product_Price") * pl.col("Units"),
            Profit=(pl.col("Product_Price") - pl.col("Product_Cost")) * pl.col("Units"),
        )
    )
    return (sales,)


@app.cell
def _(sales):
    total_orders = sales.select("Sale_ID").count().collect().item()
    total_revenue = sales.select("Revenue").sum().collect().item()
    total_profit = sales.select("Profit").sum().collect().item()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Objective 4: Build and Interactive Report
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    1. Add KPI card visuals showing "total order", "total revenue", and "total profit" for the current month, along with monthly trends for each metric.
    2. Add a slicer to filter the report page by store location.
    3. Add a bar chart showing "total orders" by product category, and a line chart showing "total revenue" with the date hierarchy on the x-axis.
    4. Assemble the charts into a logical layout and adjust formatting, alignment, and polish to finalize the report as you see fit.
    """)
    return


@app.cell
def _(stores):
    store_location_select = mo.ui.dropdown.from_series(
        stores.select("Store_Location").unique().collect().to_series(),
        label="Store Location",
    )

    store_location_select
    return (store_location_select,)


@app.function
def human_format(n, decimals=1) -> str:
    for unit in ["", "K", "M", "B", "T"]:
        if abs(n) < 1000:
            return f"{n:.{decimals}f}{unit}"
        n = n / 1000


@app.function
def filter_sales(
    sales: pl.LazyFrame, 
    store_location: str | None, 
    product_category: pl.LazyFrame | None
) -> pl.LazyFrame:
    if store_location:
        sales = sales.filter(pl.col("Store_Location") == store_location)

    if product_category:
        sales = sales.filter(pl.col("Product_Category").is_in(product_category))

    return sales


@app.function
def compute_kpis(current_sales: pl.LazyFrame, past_sales: pl.LazyFrame) -> dict:

    orders = {
        "current" : current_sales.select(pl.len()).collect().item(),
        "past": past_sales.select(pl.len()).collect().item()
    }
    revenue = {
        "current" : current_sales.select(pl.sum("Revenue")).collect().item(),
        "past": past_sales.select(pl.sum("Revenue")).collect().item()
    }
    profit = {
        "current" : current_sales.select(pl.sum("Profit")).collect().item(),
        "past": past_sales.select(pl.sum("Profit")).collect().item()
    }

    return {
        "orders": orders['current'],
        "pct_delta_orders": orders['current'] / orders['past'] - 1,
        "revenue": revenue['current'],
        "pct_delta_rev": revenue['current'] / revenue['past'] - 1,
        "profit": profit['current'],
        "pct_delta_profit": profit['current'] / profit['past'] - 1,
    }


@app.cell
def _(product_category_chart, sales, store_location_select):
    category = (
        (
            product_category_chart.value
            .select("Product_Category")
            .to_series()
            .to_list() 
        )
        if len(product_category_chart.value) > 0
        else None
    )

    filtered_sales = filter_sales(
        sales,
        store_location_select.value,
        category,
    )

    current_month_data = (
        filtered_sales
        .filter(
            pl.col("Start_Month") == pl.col("Start_Month").max()
        )
    )

    yoy_month_data = (
        filtered_sales
        .filter(
            pl.col("Start_Month") == pl.col("Start_Month").max().dt.offset_by("-1y")
        )
    )

    kpis = compute_kpis(current_month_data, yoy_month_data)
    return filtered_sales, kpis


@app.cell
def _(kpis):
    monthly_order = mo.stat(
        label="Total Orders by Month",
        bordered=True,
        value=human_format(kpis["orders"]),
        caption=f"{kpis['pct_delta_orders']:.1%} change Y-o-Y",
        direction="increase" if kpis['pct_delta_orders'] > 0 else "decrease"
    )

    monthly_revenue = mo.stat(
        label="Revenue by Month",
        bordered=True,
        value="$" + human_format(kpis["revenue"]),
        caption=f"{kpis['pct_delta_rev']:.1%} change Y-o-Y",
        direction="increase" if kpis['pct_delta_rev'] > 0 else "decrease"    
    )

    monthly_profit = mo.stat(
        label="Profit by Month",
        bordered=True,
        value="$" + human_format(kpis["profit"]),
        caption=f"{kpis['pct_delta_profit']:.1%} change Y-o-Y",
        direction="increase" if kpis['pct_delta_profit'] > 0 else "decrease"    
    )

    mo.hstack(
        [monthly_order, monthly_revenue, monthly_profit],
        widths="equal",
        gap=1,
    )
    return


@app.cell
def _(filtered_sales):
    monthly_summary = (
        filtered_sales
        .group_by("Start_Month")
        .agg(
            Orders=pl.len(),
            Revenue=pl.col("Revenue").sum(),
            Profit=pl.col("Profit").sum()
        ).collect()
    )
    return (monthly_summary,)


@app.function
def monthly_area_chart(df, y, title, y_title):
    return (
        alt.Chart(df, title=title)
        .mark_area()
        .encode(
            x=alt.X("Start_Month:T", title=""),
            y=alt.Y(f"{y}:Q", title=y_title, scale=alt.Scale(zero=False)),
            color=alt.value("lightblue"),
        )
    )


@app.cell
def _(monthly_summary):
    mo.ui.altair_chart(
        monthly_area_chart(
            monthly_summary,
            y="Orders",
            title="Orders by Month",
            y_title="Orders",
        )
    )
    return


@app.cell
def _(monthly_summary):
    mo.ui.altair_chart(
        monthly_area_chart(
            monthly_summary,
            y="Revenue",
            title="Revenue by Month",
            y_title="Revenue",
        )
    )
    return


@app.cell
def _(monthly_summary):
    mo.ui.altair_chart(
        monthly_area_chart(
            monthly_summary,
            y="Profit",
            title="Profit by Month",
            y_title="Profit",
        )
    )
    return


@app.cell
def _(sales):
    total_order_by_prod_cat = (
        sales
        .group_by("Product_Category")
        .agg(
            pl.col("Units").len()
        ).collect()
    )

    product_category_chart = mo.ui.altair_chart(
        alt.Chart(total_order_by_prod_cat, title="Orders by Product Category")
        .mark_bar()
        .encode(
            alt.X("Units", axis=alt.Axis(title="")),
            alt.Y("Product_Category", axis=alt.Axis(title=""), sort="-x"),
            color=alt.value("lightblue")
        ).properties(height=250, width=600)
    )

    product_category_chart
    return (product_category_chart,)


@app.cell
def _(monthly_summary):
    mo.ui.altair_chart(
        alt.Chart(monthly_summary, title="Revenue by Month")
        .mark_line(interpolate="monotone")
        .encode(
            alt.X("Start_Month:T", axis=alt.Axis(title="")),
            alt.Y("Revenue:Q", axis=alt.Axis(title="")),
            color=alt.value("lightblue")
        ).properties(height=250, width=600)
    )
    return


if __name__ == "__main__":
    app.run()
