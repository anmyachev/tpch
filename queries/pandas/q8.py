import pandas as pd

from queries.pandas import utils

Q_NUM = 8


def q() -> None:
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds
    part_ds = utils.get_part_ds
    region_ds = utils.get_region_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    customer_ds()
    line_item_ds()
    nation_ds()
    orders_ds()
    part_ds()
    region_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal orders_ds
        nonlocal part_ds
        nonlocal region_ds
        nonlocal supplier_ds

        customer_ds = customer_ds()
        lineitem_ds = line_item_ds()
        nation_ds = nation_ds()
        orders_ds = orders_ds()
        part_ds = part_ds()
        region_ds = region_ds()
        supplier_ds = supplier_ds()

        part_filtered = part_ds[(part_ds["p_type"] == "ECONOMY ANODIZED STEEL")]
        part_filtered = part_filtered.loc[:, ["p_partkey"]]
        lineitem_filtered = lineitem_ds.loc[:, ["l_partkey", "l_suppkey", "l_orderkey"]]
        lineitem_filtered["volume"] = lineitem_ds["l_extendedprice"] * (
            1.0 - lineitem_ds["l_discount"]
        )
        total = part_filtered.merge(
            lineitem_filtered, left_on="p_partkey", right_on="l_partkey", how="inner"
        )
        total = total.loc[:, ["l_suppkey", "l_orderkey", "volume"]]
        supplier_filtered = supplier_ds.loc[:, ["s_suppkey", "s_nationkey"]]
        total = total.merge(
            supplier_filtered, left_on="l_suppkey", right_on="s_suppkey", how="inner"
        )
        total = total.loc[:, ["l_orderkey", "volume", "s_nationkey"]]
        orders_filtered = orders_ds[
            (orders_ds["o_orderdate"] >= pd.Timestamp("1995-01-01"))
            & (orders_ds["o_orderdate"] < pd.Timestamp("1997-01-01"))
        ]
        orders_filtered["o_year"] = orders_filtered["o_orderdate"].dt.year
        orders_filtered = orders_filtered.loc[:, ["o_orderkey", "o_custkey", "o_year"]]
        total = total.merge(
            orders_filtered, left_on="l_orderkey", right_on="o_orderkey", how="inner"
        )
        total = total.loc[:, ["volume", "s_nationkey", "o_custkey", "o_year"]]
        customer_filtered = customer_ds.loc[:, ["c_custkey", "c_nationkey"]]
        total = total.merge(
            customer_filtered, left_on="o_custkey", right_on="c_custkey", how="inner"
        )
        total = total.loc[:, ["volume", "s_nationkey", "o_year", "c_nationkey"]]
        n1_filtered = nation_ds.loc[:, ["n_nationkey", "n_regionkey"]]
        n2_filtered = nation_ds.loc[:, ["n_nationkey", "n_name"]].rename(
            columns={"n_name": "nation"}
        )
        total = total.merge(
            n1_filtered, left_on="c_nationkey", right_on="n_nationkey", how="inner"
        )
        total = total.loc[:, ["volume", "s_nationkey", "o_year", "n_regionkey"]]
        total = total.merge(
            n2_filtered, left_on="s_nationkey", right_on="n_nationkey", how="inner"
        )
        total = total.loc[:, ["volume", "o_year", "n_regionkey", "nation"]]
        region_filtered = region_ds[(region_ds["r_name"] == "AMERICA")]
        region_filtered = region_filtered.loc[:, ["r_regionkey"]]
        total = total.merge(
            region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
        )
        total = total.loc[:, ["volume", "o_year", "nation"]]

        def udf(df: pd.DataFrame) -> float:
            demonimator: float = df["volume"].sum()
            df = df[df["nation"] == "BRAZIL"]
            numerator: float = df["volume"].sum()
            return round(numerator / demonimator, 2)

        total = total.groupby("o_year", as_index=False).apply(udf, include_groups=False)
        total.columns = ["o_year", "mkt_share"]
        total = total.sort_values(by=["o_year"], ascending=[True])

        return total  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
