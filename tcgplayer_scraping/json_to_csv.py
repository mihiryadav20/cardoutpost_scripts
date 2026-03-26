#!/usr/bin/env python3
"""Convert tcgplayer JSON cache files to CSV."""

import json
import csv
import os

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".cache")


def convert_details():
    with open(os.path.join(CACHE_DIR, "tcgplayer_details.json")) as f:
        data = json.load(f)

    out_path = os.path.join(CACHE_DIR, "tcgplayer_details.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "tcgplayer_id", "name", "scraped_at",
            "market_price", "lowest_price", "lowest_price_with_shipping", "median_price",
            "sellers", "listings", "max_fulfillable_qty",
            "rarity", "set_name", "set_code", "set_id",
            "foil_only", "normal_only",
            "card_type", "energy_type", "hp", "number", "stage",
            "weakness", "resistance", "retreat_cost",
            "release_date", "artist",
        ])

        for card in data["cards"]:
            pd = card.get("product_details", {})
            ca = pd.get("customAttributes", {})
            fa = pd.get("formattedAttributes", {})

            card_type = ", ".join(ca.get("cardType", [])) if isinstance(ca.get("cardType"), list) else ca.get("cardType", "")
            energy_type = ", ".join(ca.get("energyType", [])) if isinstance(ca.get("energyType"), list) else ca.get("energyType", "")

            artist = fa.get("Artist", "")

            writer.writerow([
                card.get("tcgplayer_id", ""),
                card.get("name", ""),
                card.get("scraped_at", ""),
                pd.get("marketPrice", ""),
                pd.get("lowestPrice", ""),
                pd.get("lowestPriceWithShipping", ""),
                pd.get("medianPrice", ""),
                pd.get("sellers", ""),
                pd.get("listings", ""),
                pd.get("maxFulfillableQuantity", ""),
                pd.get("rarityName", ""),
                pd.get("setName", ""),
                pd.get("setCode", ""),
                pd.get("setId", ""),
                pd.get("foilOnly", ""),
                pd.get("normalOnly", ""),
                card_type,
                energy_type,
                ca.get("hp", ""),
                ca.get("number", ""),
                ca.get("stage", ""),
                ca.get("weakness", ""),
                ca.get("resistance", ""),
                ca.get("retreatCost", ""),
                ca.get("releaseDate", ""),
                artist,
            ])

    print(f"Wrote {len(data['cards'])} rows to {out_path}")


def convert_price_history():
    with open(os.path.join(CACHE_DIR, "tcgplayer_price_history.json")) as f:
        data = json.load(f)

    out_path = os.path.join(CACHE_DIR, "tcgplayer_price_history.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "tcgplayer_id", "name",
            # Near Mint aggregates
            "nm_avg_daily_qty_sold", "nm_avg_daily_txn_count",
            "nm_weekly_qty_sold", "nm_weekly_txn_count",
            "nm_total_qty_sold", "nm_total_txn_count",
            # Near Mint latest bucket
            "nm_latest_date", "nm_market_price",
            "nm_qty_sold", "nm_txn_count",
            "nm_low_price", "nm_high_price",
            # Lightly Played aggregates
            "lp_avg_daily_qty_sold", "lp_weekly_qty_sold",
            "lp_total_qty_sold", "lp_total_txn_count",
            "lp_latest_date", "lp_market_price",
            "lp_qty_sold", "lp_txn_count",
        ])

        row_count = 0
        for card in data["cards"]:
            ph = card.get("price_history") or {}
            skus = {s.get("condition", ""): s for s in (ph.get("result") or [])}

            nm = skus.get("Near Mint", {})
            lp = skus.get("Lightly Played", {})

            nm_buckets = nm.get("buckets", [])
            lp_buckets = lp.get("buckets", [])
            nm_latest = nm_buckets[0] if nm_buckets else {}
            lp_latest = lp_buckets[0] if lp_buckets else {}

            nm_daily_qty = float(nm.get("averageDailyQuantitySold") or 0)
            nm_daily_txn = float(nm.get("averageDailyTransactionCount") or 0)
            lp_daily_qty = float(lp.get("averageDailyQuantitySold") or 0)

            writer.writerow([
                card.get("tcgplayer_id", ""),
                card.get("name", ""),
                # NM aggregates
                nm.get("averageDailyQuantitySold", ""),
                nm.get("averageDailyTransactionCount", ""),
                round(nm_daily_qty * 7),
                round(nm_daily_txn * 7),
                nm.get("totalQuantitySold", ""),
                nm.get("totalTransactionCount", ""),
                # NM latest bucket
                nm_latest.get("bucketStartDate", ""),
                nm_latest.get("marketPrice", ""),
                nm_latest.get("quantitySold", ""),
                nm_latest.get("transactionCount", ""),
                nm_latest.get("lowSalePrice", ""),
                nm_latest.get("highSalePrice", ""),
                # LP aggregates
                lp.get("averageDailyQuantitySold", ""),
                round(lp_daily_qty * 7),
                lp.get("totalQuantitySold", ""),
                lp.get("totalTransactionCount", ""),
                lp_latest.get("bucketStartDate", ""),
                lp_latest.get("marketPrice", ""),
                lp_latest.get("quantitySold", ""),
                lp_latest.get("transactionCount", ""),
            ])
            row_count += 1

    print(f"Wrote {row_count} rows to {out_path}")


if __name__ == "__main__":
    convert_details()
    convert_price_history()
