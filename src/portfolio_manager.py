import argparse
import pandas as pd
from src.db_utils import add_stock, remove_stock, get_portfolio

def main():
    parser = argparse.ArgumentParser(description="Manage your stock portfolio")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--add", nargs="+", metavar="TICKER", help="Add one or more stocks")
    group.add_argument("--remove", nargs="+", metavar="TICKER", help="Remove one or more stocks")
    group.add_argument("--list", action="store_true", help="Show current portfolio")

    args = parser.parse_args()

    if args.add:
        for tk in args.add:
            add_stock(tk.upper())
    elif args.remove:
        for tk in args.remove:
            remove_stock(tk.upper())
    elif args.list:
        df = get_portfolio()
        if isinstance(df, pd.DataFrame) and not df.empty:
            print("Current Portfolio:")
            print(df.to_string(index=False))
        else:
            print("Portfolio is empty.")

if __name__ == "__main__":
    main()
