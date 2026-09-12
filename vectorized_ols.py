import numpy as np
import pandas as pd
import statsmodels.api as sm
import time


def estimate_ar1(resid):
    """Placeholder — replace with your actual implementation."""
    x = resid[:-1]
    y = resid[1:]
    phi = np.dot(x, y) / np.dot(x, x)
    eps = y - phi * x
    sigma_eq = np.std(eps) / np.sqrt(1 - phi**2) if abs(phi) < 1 else np.nan
    return {"phi": phi, "sigma_eq": sigma_eq}


def original_loop(s1, s2, lookback):
    res_df = pd.DataFrame(
        index=s1.index, columns=["resid", "beta", "alpha", "phi", "sigma_eq"], dtype=float
    )
    for i in range(lookback, len(s1)):
        y_win = s2.iloc[i - lookback:i]
        x_win = s1.iloc[i - lookback:i]
        model = sm.OLS(y_win, sm.add_constant(x_win)).fit()
        alpha, beta = model.params

        resid_val = s2.iloc[i] - (alpha + beta * s1.iloc[i])
        res_df.iloc[i, 0] = resid_val
        res_df.iloc[i, 1] = beta
        res_df.iloc[i, 2] = alpha

        ar1_res = estimate_ar1(model.resid.values)
        res_df.iloc[i, 3] = ar1_res["phi"]
        res_df.iloc[i, 4] = ar1_res["sigma_eq"]

    return res_df


def vectorized_version(s1, s2, lookback, do_ar1=True):
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
        {"resid": resid, "beta": beta, "alpha": alpha},
        index=s1.index,
    )
    res_df["phi"] = np.nan
    res_df["sigma_eq"] = np.nan

    if do_ar1:
        for i in range(lookback, len(s1)):
            a, b = alpha.iloc[i], beta.iloc[i]
            if pd.isna(a) or pd.isna(b):
                continue
            window_resid = (
                s2.iloc[i - lookback:i] - (a + b * s1.iloc[i - lookback:i])
            ).values
            ar1_res = estimate_ar1(window_resid)
            res_df.iloc[i, 3] = ar1_res["phi"]
            res_df.iloc[i, 4] = ar1_res["sigma_eq"]

    return res_df


if __name__ == "__main__":
    np.random.seed(42)
    n = 500
    lookback = 60

    x = np.cumsum(np.random.randn(n)) + 100
    noise = np.random.randn(n) * 0.5
    y = 0.8 * x + 5 + noise

    s1 = pd.Series(x, name="x")
    s2 = pd.Series(y, name="y")

    t0 = time.time()
    df_orig = original_loop(s1, s2, lookback)
    t1 = time.time()
    df_vec = vectorized_version(s1, s2, lookback, do_ar1=True)
    t2 = time.time()

    print(f"Original loop time:      {t1 - t0:.4f}s")
    print(f"Vectorized+AR1 loop time:{t2 - t1:.4f}s")

    diff = (df_orig - df_vec).abs()
    print("\nMax absolute difference per column:")
    print(diff.max())

    print("\nAny NaN mismatch check (rows where one is NaN and other isn't):")
    nan_mismatch = df_orig.isna() != df_vec.isna()
    print(nan_mismatch.sum())

    print("\nSample comparison (rows 60-65):")
    print(pd.concat([df_orig.iloc[60:65], df_vec.iloc[60:65]], axis=1,
                     keys=["orig", "vec"]))