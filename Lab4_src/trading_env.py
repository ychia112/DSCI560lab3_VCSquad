import pandas as pd
import numpy as np        
    
class TradingEnvironment:
    def __init__(self, config=None, initial_cash=100000, mode="signals", tickers=None):
        if config is not None:
            self.initial_cash = config.initial_cash
            self.mode = config.mode
            self.transaction_cost_bps = config.transaction_cost_bps
            self.slippage_bps = config.slippage_bps
            self.lot_size = config.lot_size
            self.allow_short = config.allow_short
        else:
            self.initial_cash = initial_cash
            self.mode = mode
            self.transaction_cost_bps = 0.0
            self.slippage_bps = 0.0
            self.lot_size = 1
            self.allow_short = False
            
        self.tickers = tickers or []
        
    def _apply_cost(self, price, side):
        '''To stimulate transaction cost if there is'''
        if self.transaction_cost_bps != 0.0 or self.slippage_bps != 0.0:
            bps_cost = self.transaction_cost_bps + self.slippage_bps
            if side == 'buy':
                return price * (1 + bps_cost)
            else:
                return price * (1 - bps_cost)
        return price
        
    def run_backtest(self, prices_open, prices_close, signals_or_weights):
        cash = self.initial_cash
        shares = {tk: 0 for tk in self.tickers}
        rows = []
        
        for i in range(len(prices_open) - 1):
            t = prices_open.index[i]
            t_next = prices_open.index[i+1]
            
            if t not in signals_or_weights.index:
                continue
            
            decisions = signals_or_weights.loc[t]
            
            
            # mode: weights
            if self.mode == 'weights':
                port_val = cash + sum(
                    shares[tk] * prices_close.at[t, tk] for tk in self.tickers
                )
                target_alloc = {tk: decisions[tk] for tk in self.tickers}
                for tk in self.tickers:
                    target_val = port_val * target_alloc[tk]
                    current_val = shares[tk] * prices_close.at[t, tk]
                    diff_val = target_val - current_val
                    price = self._apply_cost(prices_open.at[t_next, tk], "buy")
                    n_shares = int(diff_val // price)
                    cash -= n_shares * price
                    shares[tk] += n_shares
                    
                    
            # mode: signals
            elif self.mode == 'signals':
                for tk in self.tickers:
                    sig = decisions[tk]
                    if sig == "buy" and cash >= prices_open.at[t_next, tk]:
                        n_shares = int(cash // prices_open.at[t_next, tk])
                        cash -= n_shares * self._apply_cost(prices_open.at[t_next, tk], "buy")
                        shares[tk] += n_shares
                    elif sig == "sell" and shares[tk] > 0:
                        cash += shares[tk] * self._apply_cost(prices_open.at[t_next, tk], "sell")
                        shares[tk] = 0

            
            # Update asset daily
            port_val = cash + sum(
                shares[tk] * prices_close.at[t, tk] for tk in self.tickers
            )
            rows.append(
                {
                    "date": t,
                    "cash": cash,
                    "total_value": port_val,
                    **{f"pos_{tk}": shares[tk] for tk in self.tickers},
                }
            )
            
        return pd.DataFrame(rows).set_index("date")

    
# Evaluate Metrics: Static methods for evaluating portfolio performance.
class Evaluation:
    @staticmethod
    def value_series(ledger):
        return ledger["total_value"].astype(float)
    
    @staticmethod
    def annualized_return(value):
        n = value.shape[0]
        if n <= 1:
            return 0.0
        total_ret = float(value.iloc[-1] / value.iloc[0]) - 1.0
        years = n / 252.0
        return (1.0 + total_ret) ** (1.0 / years) - 1.0 if years > 0 else total_ret
    
    @staticmethod
    def sharpe(value):
        r = value.pct_change().dropna()
        if len(r) == 0:
            return 0.0
        return float((r.mean() / (r.std() + 1e-12)) * np.sqrt(252))
    
    @staticmethod
    def max_drawdown(value):
        peak = value.cummax()
        dd = (value / peak) - 1.0
        return float(dd.min())