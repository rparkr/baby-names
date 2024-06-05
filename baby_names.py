"""
Baby name trends: Streamlit app for viewing popularity over time for US baby names.

Created by: [Ryan Parker](http://github.com/rparkr "See Ryan's GitHub profile")
"""

# Built-in packages
from io import BytesIO
import zipfile

# External packages
from bokeh.embed import file_html
import holoviews as hv
import polars as pl
import requests
import streamlit as st
import streamlit.components.v1 as components

hv.extension("bokeh")
pl.enable_string_cache()

st.set_page_config(
    page_title="Baby name trends",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)


@st.cache_data()
def load_data() -> pl.DataFrame:
    """
    Return a polars dataframe with processed baby names data.

    The data comes from the US Social Security Administration's
    baby names datasets and can be downloaded from data.gov.

    Sources:
    Links to all data files: https://www.ssa.gov/oact/babynames/limits.html
    SSA Baby Names main page: https://www.ssa.gov/oact/babynames/index.html
    Data.gov: National baby names: https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-data
    Data.gov: State baby names: https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-state-and-district-of-columbia-data
    """
    zip_national = BytesIO(
        requests.get("https://www.ssa.gov/oact/babynames/names.zip").content
    )
    zip_state = BytesIO(
        requests.get(
            "https://www.ssa.gov/oact/babynames/state/namesbystate.zip"
        ).content
    )
    # Work with the ZIP files in memory to extract their contents
    # References:
    # https://realpython.com/python-zipfile
    # https://stackoverflow.com/a/10909016/17005348
    # First, the State files
    with zipfile.ZipFile(zip_state, mode="r") as archive:
        df_state = pl.concat(
            items=[
                pl.read_csv(
                    source=archive.read(filename),
                    new_columns=["state", "gender", "year", "name", "count"],
                    schema={
                        "state": pl.Categorical,
                        "gender": pl.Categorical,
                        "year": pl.Int16,
                        "name": pl.Utf8,
                        "count": pl.Int32,
                    },
                    has_header=False,
                )
                for filename in archive.namelist()
                if filename.lower().endswith(".txt")
            ],
            rechunk=True,
        )
    # Next, the national files
    with zipfile.ZipFile(zip_national, mode="r") as archive:
        df_national = pl.concat(
            items=[
                pl.read_csv(
                    source=archive.read(filename),
                    new_columns=["name", "gender", "count"],
                    schema={
                        "name": pl.Utf8,
                        "gender": pl.Categorical,
                        "count": pl.Int32,
                    },
                    has_header=False,
                ).with_columns(
                    # Extract the year from the filename in the format:
                    # "yob2021.txt" where "yob" means "year of birth"
                    # followed by the year.
                    year=pl.lit(filename[3:7]).cast(pl.Int16),
                    state=pl.lit("nation").cast(pl.Categorical),
                )
                for filename in archive.namelist()
                if filename.lower().endswith(".txt")
            ],
            rechunk=True,
        ).select(["state", "gender", "year", "name", "count"])
    # Now combine both DataFrames
    df = pl.concat(items=[df_national, df_state], rechunk=True).with_columns(
        popularity=(
            pl.col("count") / pl.col("count").sum().over(["state", "gender", "year"])
        ),
        rank=pl.col("count")
        .rank(method="ordinal", descending=True)
        .over(["state", "gender", "year"]),
    )
    return df


df = load_data()
# st.write(df)
# st.write(df.estimated_size(unit="megabytes"))

years = st.sidebar.slider(
    label="Years",
    min_value=df["year"].min(),
    max_value=(max_year := df["year"].max()),
    value=(2015, max_year),
)

# Display the options
col1, col2, col3, col4 = st.columns(4)
with col1:
    first_name = col1.selectbox(
        label="Name",
        index=None,
        placeholder="Search for a first name",
        options=df["name"].unique().sort(),
    )
with col2:
    states = col2.multiselect(
        label="States", default=["nation"], options=df["state"].unique()
    )
with col3:
    use_rank = col3.toggle(
        label="Display rank",
        value=True,
        help="Toggle between displaying rank or percentage of names for that gender, year, and state",
    )
with col4:
    groupby_gender = col4.toggle(
        label="Group by gender",
        value=True,
        help="When turned off, specify the gender in the sidebar menu to analyze name trends by gender",
    )

gender_select = st.sidebar.selectbox(
    label="Gender", options=["F", "M"], index=0, disabled=groupby_gender
)

# Create the plot based on the chosen options
# Only show the plot once a name has been selected
if first_name:
    plot = df.filter(
        (pl.col("name") == first_name)
        & (pl.col("state").is_in(states))
        & (pl.col("year").is_between(*years))
        & (True if groupby_gender else (pl.col("gender") == gender_select))
    ).plot.line(
        x="year",
        y="rank" if use_rank else "popularity",
        by=["state", "gender"] if groupby_gender else "state",
        title=f"Popularity of name: {first_name}",
        flip_yaxis=True if use_rank else False,
        height=400,
        # xlim=years,
    )
    # Display the plot
    # See: https://github.com/streamlit/streamlit/issues/5858#issuecomment-1793784439
    # and: https://discourse.holoviz.org/t/get-underlying-bokeh-figure-object-back-from-hvplot/2918/2
    p = hv.render(plot, backend="bokeh")
    components.html(file_html(p, "cdn"), height=800)
else:
    info_box = st.info("Type a first name in the `Name` field above")
