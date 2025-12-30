import marimo

__generated_with = "0.18.4"
app = marimo.App(layout_file="layouts/airline-flight-delay-report.grid.json")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import altair as alt
    from typing import Optional
    from pathlib import Path


@app.cell(hide_code=True)
def _():
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

    chart_color_palette = [
        "#eaf6fb",
        "#d6edf7",
        "#c2e3f0",
        "#add8e6",
        "#8fc9dd",
        "#71b9d3",
    ]

    alt.theme.enable("marimo_light")
    return (chart_color_palette,)


@app.cell(hide_code=True)
def _():
    mo.center(mo.md("# Airline Flight Delay Report"))
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Load Data and Build a Relational Model
    """)
    return


@app.cell
def _():
    path = Path("project-portfolio/airline-flight-delay-report/airlines-airports-data")

    airlines = pl.scan_csv(path / "airlines.csv")
    airports = pl.scan_csv(path / "airports.csv")
    cancellation_codes = pl.scan_csv(path / "cancellation_codes.csv")

    flights = (
        pl.scan_parquet(path / "flights-selected.parquet")
        .with_columns(
            pl.when(pl.col("CANCELLED") == 1)
            .then(pl.lit("Canceled"))
            .otherwise(
                pl.when(pl.col("DEPARTURE_DELAY") > 0)
                .then(pl.lit("Delayed"))
                .otherwise(pl.lit("On-Time"))
            ).alias("Status")
        ).join(
            airlines, 
            left_on="AIRLINE", 
            right_on="IATA_CODE",
            how="left",
            suffix=" NAME"
        ).join(
            airports,
            left_on="ORIGIN_AIRPORT",
            right_on="IATA_CODE",
            how="left"
        ).join(
            cancellation_codes,
            on="CANCELLATION_REASON",
            how="left"
        )
    )
    return (flights,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Prepare Summary Data
    """)
    return


@app.cell
def _(flights):
    def multiselect_opt(data, column):
        return mo.ui.multiselect(options=(
            data
            .select(column)
            .unique()
            .sort(column)
            .collect()
            .to_series()
        ))

    city_multiselect = multiselect_opt(flights, "CITY")
    airline_multiselect = multiselect_opt(flights, "AIRLINE NAME")
    dow_multiselect = multiselect_opt(flights, "DAY_OF_WEEK")
    return airline_multiselect, city_multiselect, dow_multiselect


@app.cell
def _(city_multiselect):
    mo.hstack([city_multiselect, mo.md(f"Selected Cities: {city_multiselect.value}")])
    return


@app.cell
def _(airline_multiselect):
    mo.hstack([airline_multiselect, mo.md(f"Selected Airlines: {airline_multiselect.value}")])
    return


@app.cell
def _(dow_multiselect):
    mo.hstack([dow_multiselect, mo.md(f"Selected Day of Week: {dow_multiselect.value}")])
    return


@app.function
def multiselect_filter_flight(
    flight_data: pl.LazyFrame,
    city_filter: list | None,
    airline_filter: list | None,
    dow_filter: list | None,
) -> pl.LazyFrame:

    if city_filter:
        flight_data = flight_data.filter(pl.col("CITY").is_in(city_filter))

    if airline_filter:
        flight_data = flight_data.filter(pl.col("AIRLINE NAME").is_in(airline_filter))

    if dow_filter:
        flight_data = flight_data.filter(pl.col("DAY_OF_WEEK").is_in(dow_filter))

    return flight_data


@app.function
def chart_filter_flight(
    flight_data: pl.LazyFrame,
    city_filter: Optional[pl.DataFrame] = None,
    airline_filter: Optional[pl.DataFrame] = None,
    dow_filter: Optional[pl.DataFrame] = None,
) -> pl.LazyFrame:

    if city_filter is not None:
        cities = city_filter.get_column("CITY").to_list()
        if cities:
            flight_data = flight_data.filter(pl.col("CITY").is_in(cities))

    if airline_filter is not None:
        airlines = airline_filter.get_column("AIRLINE NAME").to_list()
        if airlines:
            flight_data = flight_data.filter(pl.col("AIRLINE NAME").is_in(airlines))

    if dow_filter is not None:
        days = dow_filter.get_column("DAY_OF_WEEK").to_list()
        if days:
            flight_data = flight_data.filter(pl.col("DAY_OF_WEEK").is_in(days))

    return flight_data


@app.cell
def _(airline_multiselect, city_multiselect, dow_multiselect, flights):
    filtered_flights = multiselect_filter_flight(
        flights,
        city_multiselect.value,
        airline_multiselect.value,
        dow_multiselect.value
    )
    return (filtered_flights,)


@app.cell
def _(
    canceled_flights_week_chart,
    filtered_flights,
    flights_by_city_chart,
    pct_delay_airline_chart,
):
    chart_filtered_flights = chart_filter_flight(
        filtered_flights,
        city_filter=flights_by_city_chart.value,
        airline_filter=pct_delay_airline_chart.value,
        dow_filter=canceled_flights_week_chart.value
    )
    return (chart_filtered_flights,)


@app.cell
def _(chart_filtered_flights):
    monthly_flights = (
        chart_filtered_flights
        .group_by("MONTH")
        .agg(
            pl.col("Status")
            .len()
            .alias("total"),
            pl.col("Status")
            .filter(pl.col("Status") == "On-Time")
            .count()
            .alias("ontime"),
            pl.col("Status")
            .filter(pl.col("Status") == "Delayed")
            .count()
            .alias("delayed"),         
            pl.col("Status")
            .filter(pl.col("Status") == "Canceled")
            .count()
            .alias("canceled")
        ).with_columns(
            pct_ontime = pl.col("ontime") / pl.col("total"),
            pct_delayed = pl.col("delayed") / pl.col("total"),
            pct_canceled = pl.col("canceled") / pl.col("total")
        )
        .sort("MONTH", descending=False)
        .collect()
    )
    return (monthly_flights,)


@app.cell
def _(filtered_flights):
    flights_by_city = (
        filtered_flights
        .filter(pl.col("CITY").is_not_null())
        .group_by("CITY")
        .agg(total=pl.len())
        .sort("total", descending=True)
        .head(10)
        .collect()
    )
    return (flights_by_city,)


@app.cell
def _(filtered_flights):
    pct_delay_airline = (
        filtered_flights
        .group_by("AIRLINE NAME")
        .agg(
            pl.col("Status").len().alias("total"),
            pl.col("Status")
            .filter(pl.col("Status") == "Delayed")
            .count()
            .alias("delayed")
        )
        .with_columns(
            pct_delayed = pl.col("delayed") / pl.col("total")
        )
        .sort("pct_delayed", descending=True)
        .head(10)
        .collect()
    )
    return (pct_delay_airline,)


@app.cell
def _(filtered_flights):
    canceled_flights_week_summary = (
        filtered_flights
        .filter(pl.col("Status") == "Canceled")
        .group_by("DAY_OF_WEEK")
        .agg(
            pl.col("Status").len().alias("canceled"),
        )
        .with_columns(
            pct_total = pl.col("canceled") / pl.col("canceled").sum()
        )
        .sort("DAY_OF_WEEK")
        .collect()
    )
    return (canceled_flights_week_summary,)


@app.cell
def _(chart_filtered_flights):
    canceled_flights_summary = (
        chart_filtered_flights
        .filter(pl.col("CANCELLATION_DESCRIPTION").is_not_null())
        .group_by("CANCELLATION_DESCRIPTION")
        .agg(
            pl.col("Status").len().alias("canceled"),
        )
        .with_columns(
            pct_total = pl.col("canceled") / pl.col("canceled").sum()
        )
        .collect()
    )
    return (canceled_flights_summary,)


@app.cell
def _(chart_filtered_flights):
    flight_status_summary = (
        chart_filtered_flights
        .group_by("Status")
        .agg(
            pl.len().alias("Total")
        )
        .with_columns(
            (pl.col("Total") / pl.col("Total").sum()).alias("% of Total")
        )
        .collect()
    )
    return (flight_status_summary,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Compute & Display KPIs/Stats
    """)
    return


@app.function
def compute_kpis(flights_data: pl.LazyFrame) -> dict:

    def count(data, filter = None):
        if filter:
            data = data.filter(pl.col("Status") == filter)
        return (
            data
            .select("Status")
            .count()
            .item()
        )
    flights_data = flights_data.collect()
    total_flights = count(flights_data)
    ontime_flights = count(flights_data, "On-Time")    
    delayed_flights = count(flights_data, "Delayed")
    canceled_flights = count(flights_data, "Canceled")

    def safe_pct(numerator, denominator):
        return None if denominator == 0 else numerator / denominator

    return {
        "total": total_flights,
        "ontime": ontime_flights,
        "pct_ontime": safe_pct(ontime_flights, total_flights),
        "delayed": delayed_flights,
        "pct_delayed": safe_pct(delayed_flights, total_flights),
        "canceled": canceled_flights,
        "pct_canceled": safe_pct(canceled_flights, total_flights),
    }


@app.function
def human_format(n, decimals=1) -> str:
    for unit in ["", "K", "M", "B", "T"]:
        if abs(n) < 1000:
            return f"{n:.{decimals}f}{unit}"
        n = n / 1000


@app.cell
def _(chart_filtered_flights):
    kpis = compute_kpis(chart_filtered_flights)
    return (kpis,)


@app.cell
def _(kpis):
    total_flights = mo.stat(
        label="Total Flights",
        value=human_format(kpis["total"], 2),
    )

    delayed_flights = mo.stat(
        label="Delayed Flights",
        value=human_format(kpis["delayed"], 2),
    )

    canceled_flights = mo.stat(
        label="Canceled Flights",
        value=human_format(kpis["canceled"]),
    )

    pct_ontime = mo.stat(
        label="On-Time",
        value=f"{kpis['pct_ontime']:.0%}",
    )

    pct_delayed = mo.stat(
        label="Delayed",
        value=f"{kpis['pct_delayed']:.0%}",
    )

    pct_canceled = mo.stat(
        label="Canceled",
        value=f"{kpis['pct_canceled']:.0%}",
    )
    return (
        canceled_flights,
        delayed_flights,
        pct_canceled,
        pct_delayed,
        pct_ontime,
        total_flights,
    )


@app.cell
def _(total_flights):
    total_flights
    return


@app.cell
def _(delayed_flights):
    delayed_flights
    return


@app.cell
def _(canceled_flights):
    canceled_flights
    return


@app.cell
def _(pct_canceled, pct_delayed, pct_ontime):
    mo.hstack(
        [pct_ontime, pct_delayed, pct_canceled],
        widths="equal",
        gap=0,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Display Charts
    """)
    return


@app.function
def monthly_trendline_chart(data, field, relative_field):
    base = alt.Chart(data).encode(
        x=alt.X(
            "MONTH:Q",
            scale=alt.Scale(zero=False),
            axis=alt.Axis(labels=False, title=None, ticks=False)),
        y=alt.Y(
            f"{field}:Q",
            scale=alt.Scale(zero=False),
            axis=alt.Axis(labels=False, title=None, ticks=False)),
        color=alt.value("lightblue")
    )

    line = base.mark_line(
        interpolate="monotone",
        strokeWidth=2
    )

    if relative_field == "pct_ontime":
        rel_title = "% On-Time"
    elif relative_field == "pct_delayed":
        rel_title = "% Delayed"
    else:
        rel_title = "% Canceled"

    points = base.mark_point(
        opacity=0
    ).encode(
        tooltip=[
            alt.Tooltip("MONTH:Q", title="Month"),
            alt.Tooltip(f"{field}:Q", title="Flights", format=","),
            alt.Tooltip(f"{relative_field}:Q", title=rel_title, format=".1%")           
        ]
    )    

    return (line + points).properties(height=80, width=250)


@app.cell
def _(monthly_flights):
    total_chart = monthly_trendline_chart(monthly_flights, "total", "pct_ontime")
    mo.ui.altair_chart(total_chart)
    return


@app.cell
def _(monthly_flights):
    delayed_chart = monthly_trendline_chart(monthly_flights, "delayed", "pct_delayed")
    mo.ui.altair_chart(delayed_chart)
    return


@app.cell
def _(monthly_flights):
    canceled_chart = monthly_trendline_chart(monthly_flights, "canceled", "pct_canceled")
    mo.ui.altair_chart(canceled_chart)
    return


@app.cell
def _(flights_by_city):
    flights_by_city_chart = mo.ui.altair_chart(
        alt.Chart(flights_by_city)
        .mark_bar()
        .encode(
            alt.X("total", axis=alt.Axis(title="")),
            alt.Y("CITY", axis=alt.Axis(title=""), sort="-x"),
            color=alt.value("lightblue"),
            tooltip=[
                alt.Tooltip("CITY", title="City"),
                alt.Tooltip("total", title="Total Flights", format=",")
            ]
        ).properties(height=450, width=250)
    )
    flights_by_city_chart
    return (flights_by_city_chart,)


@app.cell
def _(pct_delay_airline):
    pct_delay_airline_chart = mo.ui.altair_chart(
        alt.Chart(pct_delay_airline)
        .mark_bar()
        .encode(
            alt.X("pct_delayed", axis=alt.Axis(title="")),
            alt.Y("AIRLINE NAME", axis=alt.Axis(title=""), sort="-x"),
            color=alt.value("lightblue"),
            tooltip=[
                alt.Tooltip("AIRLINE NAME", title="Airline"),
                alt.Tooltip("pct_delayed", title="% Delayed", format=".1%")
            ]
        ).properties(height=450, width=210)
    )
    pct_delay_airline_chart
    return (pct_delay_airline_chart,)


@app.cell
def _(canceled_flights_summary, chart_color_palette):
    mo.ui.altair_chart(
        alt.Chart(canceled_flights_summary) 
        .mark_arc(innerRadius=70) 
        .encode( 
            theta="canceled", 
            color=alt.Color(
                "CANCELLATION_DESCRIPTION:N",
                legend=alt.Legend(title="Cancellation Reason"),
                scale=alt.Scale(range=chart_color_palette)
            ),
            tooltip=[
                alt.Tooltip("CANCELLATION_DESCRIPTION:N", title="Reason"),
                alt.Tooltip("canceled:Q", title="Flights", format=","),
                alt.Tooltip("pct_total:Q", title="% of Cancellation", format=".1%")
            ]        
        ).properties(height=220, width=260)
    )
    return


@app.cell
def _(canceled_flights_week_summary):
    canceled_flights_week_chart = mo.ui.altair_chart(
        alt.Chart(canceled_flights_week_summary)
        .mark_bar()
        .encode(
            alt.X("DAY_OF_WEEK:O", axis=alt.Axis(title="")),
            alt.Y("canceled", axis=alt.Axis(title=""), sort="-x"),
            color=alt.value("lightblue"),
            tooltip=[
                alt.Tooltip("DAY_OF_WEEK:O", title="Day of Week"),
                alt.Tooltip("canceled", title="Canceled Flights", format=","),
                alt.Tooltip("pct_total", title="% of Total", format=".1%")
            ]
        ) .properties(height=220, width=350)
    )
    canceled_flights_week_chart
    return (canceled_flights_week_chart,)


@app.cell
def _(chart_color_palette, flight_status_summary):
    alt.Chart(flight_status_summary).mark_bar().encode(
        x=alt.X(
            "% of Total:Q",
            stack="normalize",
            scale=alt.Scale(domain=[0, 1]),
            axis=alt.Axis(
                format="%",
                tickCount=4,
                title=""
            ),
        ),
        color=alt.Color(
            "Status",
            legend=None,
            scale=alt.Scale(range=chart_color_palette)
            ),
        tooltip=[
            alt.Tooltip("Status", title="Flight Status"),
            alt.Tooltip("% of Total", title="% of Total", format=".1%"),
            alt.Tooltip("Total", title="Total", format=",")
            ]
    )
    return


if __name__ == "__main__":
    app.run()
