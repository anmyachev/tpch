from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from queries.modin import utils

if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 5


def q() -> None:
    date1 = date(1994, 1, 1)
    date2 = date(1995, 1, 1)

    region_ds = utils.get_region_ds
    nation_ds = utils.get_nation_ds
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # first call one time to cache in case we don't include the IO times
    region_ds()
    nation_ds()
    customer_ds()
    line_item_ds()
    orders_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal region_ds
        nonlocal nation_ds
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        region_ds = region_ds()
        nation_ds = nation_ds()
        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()
        supplier_ds = supplier_ds()

        rsel = region_ds.r_name == "ASIA"
        osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
        forders = orders_ds[osel]
        fregion = region_ds[rsel]
        fregion.rename(columns={"r_regionkey": "test1"}, inplace=True)
        nation_ds.rename(columns={"n_regionkey": "test1"}, inplace=True)
        jn1 = fregion.merge(nation_ds, on="test1")
        customer_ds.rename(columns={"c_nationkey": "test2"}, inplace=True)
        jn1.rename(columns={"n_nationkey": "test2"}, inplace=True)
        jn2 = jn1.merge(customer_ds, on="test2")
        jn2.rename(columns={"c_custkey": "test3"}, inplace=True)
        forders.rename(columns={"o_custkey": "test3"}, inplace=True)
        jn3 = jn2.merge(forders, on="test3")
        jn3.rename(columns={"o_orderkey": "test4"}, inplace=True)
        line_item_ds.rename(columns={"l_orderkey": "test4"}, inplace=True)
        jn4 = jn3.merge(line_item_ds, on="test4")

        supplier_ds.rename(columns={"s_suppkey": "test5", "s_nationkey": "test6"}, inplace=True)
        jn4.rename(columns={"l_suppkey": "test5", "test2": "test6"}, inplace=True)
        jn5 = supplier_ds.merge(
            jn4,
            on=["test5", "test6"],
        )
        jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)
        gb = jn5.groupby("n_name", as_index=False)["revenue"].sum()
        result_df = gb.sort_values("revenue", ascending=False)
        return result_df  # type: ignore[no-any-return]

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()

