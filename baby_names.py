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
# hv.extension("plotly")
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


def find_matching_rows(
    df: pl.DataFrame, text: str, years: list, states: list, starts_with: bool = False
) -> pl.Series:
    """Return a boolean series of matching rows"""
    result = (
        (
            (df["name"] == text.capitalize())
            if not starts_with
            else (df["name"].str.starts_with(text.capitalize()))
        )
        & (df["year"].is_in(years))
        & (df["state"].is_in(states))
    )
    return result


# For ranking the returned results, see: https://github.com/bnm3k/polars-fuzzy-match?tab=readme-ov-file
def filter_name(
    df: pl.DataFrame, col: str, text: str, n_rows: int = 10
) -> pl.DataFrame:
    """Filter a column of the dataframe and return matches."""
    mask = find_matching_rows(df, text, years, states, starts_with=True)
    return df[col].filter(mask).unique().sort().head(n_rows)


def check_match(df: pl.DataFrame, text: str, years: list, states: list) -> bool:
    result = find_matching_rows(df, text, years, states)
    return result.any()


df = load_data()
# st.write(df.estimated_size(unit="megabytes"))


def show_matches(key):
    matches.dataframe(
        pl.DataFrame(
            filter_name(df, col="name", text=st.session_state[key], n_rows=10)
        ),
        use_container_width=True,
        hide_index=True,
    )


# Display the options
col1, col2, col3, col4 = st.columns(4)
with col1:
    first_name = col1.text_input(
        label="Name",
        value=None,
        key="first_name_input",
        # on_change=show_matches,
        # args=("first_name_input",)
    )
    # Selectbox version with autocomplete (slow because the list is so long)
    # first_name = col1.selectbox(
    #     label="Name",
    #     index=None,
    #     placeholder="Search for a first name",
    #     options=df["name"].unique().sort(),
    # )
    matches = col1.container(height=100)
with col2:
    states = col2.multiselect(
        label="States", default=["nation"], options=df["state"].unique()
    )
with col3:
    gender_select = col3.multiselect(
        label="Gender",
        default=["F", "M"],
        options=["F", "M"],
        help="Select the gender",
    )
with col4:
    use_rank = col4.toggle(
        label="Metric: rank",
        value=True,
        help="Toggle between displaying rank or percentage of names for that gender, year, and state",
    )

years = st.slider(
    label="Years",
    min_value=df["year"].min(),
    max_value=(max_year := df["year"].max()),
    value=(2015, max_year),
)

# In case the user removes all values, reset them
if not gender_select:
    gender_select.value = ["F", "M"]

# Create the plot based on the chosen options
# Only show the plot once a name has been selected
if first_name:
    show_matches("first_name_input")
    if check_match(df, first_name, years, states):
        plot = df.filter(
            (pl.col("name") == first_name.capitalize())
            & (pl.col("state").is_in(states))
            & (pl.col("year").is_between(*years))
            & (pl.col("gender").is_in(gender_select))
        ).plot.line(
            x="year",
            y="rank" if use_rank else "popularity",
            by=["state", "gender"],
            title=f"Popularity of name: {first_name.capitalize()}"
            if len(gender_select) != 1
            else f"Popularity of name: {first_name.capitalize()} ({gender_select[0]})",
            flip_yaxis=True if use_rank else False,
            # width=800,
            # height=600,
            responsive=True,
            # xlim=years,
        )
        # Display the plot
        # See: https://github.com/streamlit/streamlit/issues/5858#issuecomment-1793784439
        # and: https://discourse.holoviz.org/t/get-underlying-bokeh-figure-object-back-from-hvplot/2918/2
        p = hv.render(plot, backend="bokeh")
        components.html(file_html(p, "cdn"), height=400)
        # plot_displayed = st.plotly_chart(hv.render(plot, backend="plotly"))
    else:
        st.write(
            f"Name **{first_name}** not found. Try searching for a different name."
        )
else:
    info_box = st.info("Type a first name in the `Name` field above")
