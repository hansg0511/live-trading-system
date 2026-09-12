import pandas as pd
import numpy as np
import statsmodels.api as sm
from typing import Dict, Optional

def estimate_ar1(series: pd.Series) -> Dict[str, float]:
    """
    Estimate AR(1) parameters for a mean-reverting series.
    S_t = c + phi * S_{t-1} + epsilon_t
    """
    x = series.dropna().values
    if len(x) < 10:
        return {'phi': np.nan, 'sigma_epsilon': np.nan, 'sigma_eq': np.nan, 'half_life': np.nan}
    
    y = x[1:]
    x_lag = x[:-1]
    X = sm.add_constant(x_lag)
    
    model = sm.OLS(y, X).fit()
    # model.params[0] is c (intercept), model.params[1] is phi (slope)
    phi = model.params[1]
    sigma_epsilon = np.sqrt(model.mse_resid)
    
    # Derived quantities
    if abs(phi) < 1:
        sigma_eq = sigma_epsilon / np.sqrt(1 - phi**2)
        # half_life: t s.t. phi^t = 0.5 => t = ln(0.5)/ln(phi)
        # Approximation for phi near 1: t = ln(2)/(1-phi)
        half_life = np.log(2) / (1 - phi) if phi < 1 else np.nan
    else:
        sigma_eq = np.nan
        half_life = np.nan
            
    return {
        'phi': phi,
        'sigma_epsilon': sigma_epsilon,
        'sigma_eq': sigma_eq,
        'half_life': half_life
    }

def estimate_half_life(series: pd.Series) -> float:
    """Estimate half-life of a mean-reverting series using the AR(1) approach."""
    res = estimate_ar1(series)
    return res['half_life']

def compute_residuals(stock1: pd.Series, stock2: pd.Series, lookback: Optional[int] = None, log_space: bool = False) -> pd.DataFrame:
    """
    Compute residuals of the regression stock2 = alpha + beta * stock1.
    stock1 is independent (X), stock2 is dependent (Y).
    If log_space is True, performs regression on ln(S2) = alpha + beta * ln(S1).
    Returns a DataFrame with columns ['residual', 'hedge_ratio', 'intercept', 'phi', 'sigma_eq'].
    """
    # Align series
    combined = pd.concat([stock1, stock2], axis=1).dropna()
    s1, s2 = combined.iloc[:, 0], combined.iloc[:, 1]
    
    if log_space:
        s1, s2 = np.log(s1), np.log(s2)

    res_df = pd.DataFrame(index=s1.index, columns=['residual', 'hedge_ratio', 'intercept', 'phi', 'sigma_eq'], dtype=float)

    if lookback is None:
        # Full sample OLS
        X = sm.add_constant(s1)
        model = sm.OLS(s2, X).fit()
        res_df['residual'] = model.resid
        res_df['hedge_ratio'] = model.params[1]
        res_df['intercept'] = model.params[0]
        
        ar1_res = estimate_ar1(model.resid)
        res_df['phi'] = ar1_res['phi']
        res_df['sigma_eq'] = ar1_res['sigma_eq']
        return res_df
            
    for i in range(lookback, len(s1)):
        # Use window ending at i-1 to predict value at i (no lookahead)
        y_win = s2.iloc[i - lookback:i]
        x_win = s1.iloc[i - lookback:i]
        model = sm.OLS(y_win, sm.add_constant(x_win)).fit()
        alpha, beta = model.params
        
        resid_val = s2.iloc[i] - (alpha + beta * s1.iloc[i])
        res_df.iloc[i, 0] = resid_val
        res_df.iloc[i, 1] = beta
        res_df.iloc[i, 2] = alpha
        
        # Estimate AR1 on the rolling residuals to get phi and sigma_eq
        ar1_res = estimate_ar1(model.resid)
        res_df.iloc[i, 3] = ar1_res['phi']
        res_df.iloc[i, 4] = ar1_res['sigma_eq']

    return res_df

def compute_rolling_ols_signal(s1, s2, lookback, estimate_ar1_fn, do_ar1=True):
    """
    Vectorized replacement for the per-bar sm.OLS rolling loop.

    s1, s2         : price series (x = s1, y = s2), same index
    lookback       : rolling window length
    estimate_ar1_fn: your existing estimate_ar1 function, passed in
                      (avoids re-implementing/duplicating it here)
    do_ar1         : set False to skip phi/sigma_eq entirely if unused downstream

    Returns a DataFrame indexed like s1/s2 with columns:
        residual, hedge_ratio, intercept, phi, sigma_eq
    Semantics match the original loop exactly:
        - beta/alpha at row i are fit on window [i-lookback, i-1]
        - resid at row i = s2[i] - (alpha_i + beta_i * s1[i])   (walk-forward, no lookahead)
        - phi/sigma_eq at row i are estimated on IN-SAMPLE residuals
          of window [i-lookback, i-1] using that window's own alpha/beta
          (i.e. same definition as model.resid in the original sm.OLS loop)
    """
    roll_x = s1.rolling(lookback)

    mean_x = roll_x.mean()
    mean_y = s2.rolling(lookback).mean()
    cov_xy = roll_x.cov(s2)
    var_x = roll_x.var()

    raw_beta = cov_xy / var_x
    raw_alpha = mean_y - raw_beta * mean_x

    beta = raw_beta.shift(1)
    alpha = raw_alpha.shift(1)
    resid = s2 - (alpha + beta * s1)

    res_df = pd.DataFrame(
        {"residual": resid, "hedge_ratio": beta, "intercept": alpha},
        index=s1.index,
    )
    res_df["phi"] = np.nan
    res_df["sigma_eq"] = np.nan

    if do_ar1:
        n = len(s1)
        for i in range(lookback, n):
            a, b = alpha.iloc[i], beta.iloc[i]
            if pd.isna(a) or pd.isna(b):
                continue
            window_resid = (
                s2.iloc[i - lookback:i] - (a + b * s1.iloc[i - lookback:i])
            )
            ar1_res = estimate_ar1_fn(window_resid)
            res_df.iloc[i, 3] = ar1_res["phi"]
            res_df.iloc[i, 4] = ar1_res["sigma_eq"]

    return res_df

def compute_zscore(spread: pd.Series, lookback: Optional[int] = None) -> pd.Series:
    """Compute Z-score of a spread series."""
    if lookback is None:
        mu, sigma = spread.mean(), spread.std()
        return (spread - mu) / (sigma + 1e-10)
    else:
        rolling_mean = spread.rolling(lookback).mean()
        rolling_std = spread.rolling(lookback).std()
        return (spread - rolling_mean) / (rolling_std + 1e-10)
