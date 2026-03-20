# Recove Trends

The primary objective of this pipeline is to **identify popular clothing items** by analyzing raw user search queries on the Recove platform.

## Run dagster

```
dagster dev -f scripts/pipeline.py
```

## The Engagement Score
To determine what is truly trending, we don't just count searches. Items are ranked by a custom **Engagement Score**, which is derived from:
* **Conversion Events:** Weighted actions like user click-outs and saved items.
* **User Diversity:** Ensures trends are driven by a wide audience, not just a few highly active users.

$$
S = \left( \sum_{i} \left[ \left( \text{Clicks}_i \cdot w_{\text{click}} + \text{Saves}_i \cdot w_{\text{save}} \right) \times \begin{cases} w_{\text{image}} & \text{if image area} \\ 1 & \text{otherwise} \end{cases} \right] \right) \times U_{\text{converting}}
$$

## Pipeline Output
When this project runs, it produces:
1. **BigQuery Table:** A clean, aggregated table containing the most popular clothing items (n-grams) over a dynamic lookback window (default: last 24 hours).
2. **Slack Automation:** This dbt project is designed to be orchestrated by Dagster, which triggers the run and pushes the final trending results directly to Slack.