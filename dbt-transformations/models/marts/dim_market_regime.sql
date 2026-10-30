{{
    config(
        materialized='table',
        schema='gold'
    )
}}

select
    regime_data.market_regime_key,
    regime_data.regime_code,
    regime_data.regime_name,
    regime_data.description
from (
    values
        (1, 'BULL_TREND', 'Bullish Trending Market', 'Strong upward momentum with fast moving average exceeding slower averages'),
        (2, 'BEAR_TREND', 'Bearish Trending Market', 'Strong downward momentum with fast moving average below slower averages'),
        (3, 'HIGH_VOLATILITY_BREAKOUT', 'High Volatility Breakout', 'Rapid price expansion outside Bollinger Bands with heightened realized volatility'),
        (4, 'CONSOLIDATION', 'Consolidation / Range-Bound', 'Price contained within narrow Bollinger Bands with compressed realized volatility'),
        (5, 'NEUTRAL', 'Neutral Market', 'No distinct trending or high-volatility regime identified')
) as regime_data(market_regime_key, regime_code, regime_name, description)
