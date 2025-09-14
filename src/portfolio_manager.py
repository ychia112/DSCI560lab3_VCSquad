import argparse
import pandas as pd
from typing import Optional, List
from .db_utils import get_portfolio, add_stock, remove_stock, get_latest_price, get_all_latest_prices


class PortfolioManager:
    """Portfolio management class for handling user investment portfolios"""
    
    def __init__(self):
        """Initialize Portfolio Manager"""
        pass
    
    def show_portfolio(self) -> pd.DataFrame:
        """Display current portfolio holdings"""
        try:
            portfolio_df = get_portfolio()
            if portfolio_df.empty:
                print("Your portfolio is currently empty")
                return portfolio_df
            
            print("Your Portfolio:")
            print("-" * 50)
            for _, row in portfolio_df.iterrows():
                print(f"- {row['ticker']} (Added: {row['added_at']})")
            
            return portfolio_df
        except Exception as e:
            print(f"Error retrieving portfolio: {e}")
            return pd.DataFrame()
    
    def add_to_portfolio(self, ticker: str) -> bool:
        """Add a stock to the portfolio"""
        try:
            ticker = ticker.upper().strip()
            print(f"Adding {ticker} to portfolio...")
            
            add_stock(ticker)
            print(f"Successfully added {ticker} to portfolio!")
            return True
        except Exception as e:
            print(f"Error adding {ticker}: {e}")
            return False
    
    def remove_from_portfolio(self, ticker: str) -> bool:
        """Remove a stock from the portfolio"""
        try:
            ticker = ticker.upper().strip()
            print(f"Removing {ticker} from portfolio...")
            
            remove_stock(ticker)
            print(f"Successfully removed {ticker} from portfolio!")
            return True
        except Exception as e:
            print(f"Error removing {ticker}: {e}")
            return False
    
    def batch_add(self, tickers: List[str]) -> None:
        """Add multiple stocks to portfolio in batch"""
        print(f"Batch adding {len(tickers)} stocks...")
        success_count = 0
        
        for ticker in tickers:
            if self.add_to_portfolio(ticker):
                success_count += 1
        
        print(f"Batch operation completed! Successfully added {success_count}/{len(tickers)} stocks")
    
    def get_portfolio_with_prices(self) -> pd.DataFrame:
        """Get portfolio holdings with latest prices"""
        try:
            portfolio_df = get_portfolio()
            if portfolio_df.empty:
                print("Your portfolio is currently empty")
                return pd.DataFrame()
            
            # Get all latest prices
            latest_prices = get_all_latest_prices()
            
            # Merge portfolio and price data
            result = portfolio_df.merge(
                latest_prices[['ticker', 'dt', 'close', 'volume']], 
                on='ticker', 
                how='left'
            ).rename(columns={
                'dt': 'last_update',
                'close': 'current_price'
            })
            
            print("Portfolio with Current Prices:")
            print("-" * 80)
            for _, row in result.iterrows():
                price_str = f"${row['current_price']:.2f}" if pd.notna(row['current_price']) else "N/A"
                update_str = row['last_update'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['last_update']) else "N/A"
                print(f"- {row['ticker']:<6} | Price: {price_str:<8} | Updated: {update_str}")
            
            return result
            
        except Exception as e:
            print(f"Error retrieving portfolio prices: {e}")
            return pd.DataFrame()
    
    def portfolio_summary(self) -> dict:
        """Generate portfolio summary statistics"""
        try:
            portfolio_with_prices = self.get_portfolio_with_prices()
            if portfolio_with_prices.empty:
                return {}
            
            summary = {
                'total_stocks': len(portfolio_with_prices),
                'stocks_with_data': portfolio_with_prices['current_price'].notna().sum(),
                'stocks_without_data': portfolio_with_prices['current_price'].isna().sum(),
                'average_price': portfolio_with_prices['current_price'].mean(),
                'total_value': portfolio_with_prices['current_price'].sum()
            }
            
            print("Portfolio Summary:")
            print("-" * 40)
            print(f"Total stocks: {summary['total_stocks']}")
            print(f"Stocks with price data: {summary['stocks_with_data']}")
            print(f"Stocks without price data: {summary['stocks_without_data']}")
            if pd.notna(summary['average_price']):
                print(f"Average price: ${summary['average_price']:.2f}")
                print(f"Total portfolio value: ${summary['total_value']:.2f}")
            
            return summary
            
        except Exception as e:
            print(f"Error generating portfolio summary: {e}")
            return {}


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Portfolio Management System")
    subparsers = parser.add_subparsers(dest='action', help='Available actions')
    
    # Show portfolio
    subparsers.add_parser('show', help='Display portfolio holdings')
    subparsers.add_parser('list', help='Display portfolio holdings (alias for show)')
    
    # Add stock
    add_parser = subparsers.add_parser('add', help='Add stock to portfolio')
    add_parser.add_argument('ticker', help='Stock ticker symbol')
    
    # Remove stock
    remove_parser = subparsers.add_parser('remove', help='Remove stock from portfolio')
    remove_parser.add_argument('ticker', help='Stock ticker symbol')
    
    # Batch add
    batch_parser = subparsers.add_parser('batch-add', help='Add multiple stocks to portfolio')
    batch_parser.add_argument('tickers', nargs='+', help='List of stock ticker symbols')
    
    # Portfolio with prices
    subparsers.add_parser('prices', help='Display portfolio with current prices')
    
    # Portfolio summary
    subparsers.add_parser('summary', help='Display portfolio summary statistics')
    
    return parser.parse_args()


def main():
    """Main function to handle command line interface"""
    args = parse_args()
    pm = PortfolioManager()
    
    if not args.action:
        print("Please specify an action. Use --help to see available options")
        return
    
    if args.action in ['show', 'list']:
        pm.show_portfolio()
    
    elif args.action == 'add':
        pm.add_to_portfolio(args.ticker)
    
    elif args.action == 'remove':
        pm.remove_from_portfolio(args.ticker)
    
    elif args.action == 'batch-add':
        pm.batch_add(args.tickers)
    
    elif args.action == 'prices':
        pm.get_portfolio_with_prices()
    
    elif args.action == 'summary':
        pm.portfolio_summary()


if __name__ == "__main__":
    main()