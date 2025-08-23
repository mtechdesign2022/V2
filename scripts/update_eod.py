@@
-def to_yf(sym: str) -> str:
-    return f"{str(sym).strip().str.upper() if hasattr(sym,'strip') else str(sym).upper()}.NS"
+def to_yf(sym: str) -> str:
+    # sym is a plain string like 'RELIANCE' -> 'RELIANCE.NS'
+    return f"{str(sym).strip().upper()}.NS"
@@
-def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
-    # If MultiIndex columns like ('Price','Close'), keep the last level -> 'Close'
-    if isinstance(df.columns, pd.MultiIndex):
-        df.columns = [c[-1] for c in df.columns]
-    # Normalize capitalization and spacing just in case
-    df.columns = [str(c).strip().title() for c in df.columns]
-    return df
+def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
+    """
+    Robustly flatten yfinance's many possible column layouts to single level:
+    want exactly: ['Open','High','Low','Close','Adj Close'(opt),'Volume']
+    """
+    KNOWN = {"open": "Open", "high": "High", "low": "Low",
+             "close": "Close", "adj close": "Adj Close", "volume": "Volume"}
+    if isinstance(df.columns, pd.MultiIndex):
+        newcols = []
+        for col in df.columns:
+            parts = [str(x).strip() for x in col if x is not None]
+            picked = None
+            # Prefer any part that matches a known OHLCV name
+            for p in parts:
+                key = p.lower()
+                if key in KNOWN:
+                    picked = KNOWN[key]
+                    break
+            # Common shape: ('Price','Close','^NSEI') -> pick 'Close'
+            if picked is None and len(parts) >= 2 and parts[0].lower() == "price":
+                picked = parts[1].title()
+            if picked is None:
+                picked = parts[-1].title()
+            newcols.append(picked)
+        df.columns = newcols
+    else:
+        df.columns = [str(c).strip().title() for c in df.columns]
+    return df
@@
-    return yf.download(
+    return yf.download(
         ticker,
         start=start.date(), end=end.date(),
         progress=False,
-        auto_adjust=False,   # we want raw OHLCV; adjust yourself later if needed
+        auto_adjust=False,   # consistent raw OHLCV
         actions=False,
-        group_by="column",   # ensures single-level columns for single tickers
+        # yfinance sometimes produces a MultiIndex even for single tickers;
+        # leaving group_by default and flattening ourselves is most reliable.
         timeout=60
     )
