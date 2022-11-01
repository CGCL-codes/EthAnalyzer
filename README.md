# EthAnalyzer

This repository is the implementation of the paper "Demystifying Ethereum Account Diversity: Observations, Models and Analysis" FCS'22.

The benefit of investigating diversity are multi-fold, including understand the Ethereum ecosystem deeper and open the possibility of tracking certain abnormal activities. Unfortunately, the exploration of blockchain account diversity remains scarce. Even the most relevant studies, which focus on the deanonymization of the accounts on Bitcoin, can hardly be applied on Ethereum since their underlying protocols and user idioms are different.

## Observation

The key observation is that different accounts exhibit diverse behavior patterns, leading us to propose the heuristics for classification as the premise. We then raise the coverage rate of classification by the statistical learning model Maximum Likelihood Estimation (MLE). We collect real-world data through extensive efforts to evaluate our proposed method and show its effectiveness. Furthermore, we make an in-depth analysis of the dynamic evolution of the Ethereum ecosystem and uncover the abnormal arbitrage actions.  We validate two sweeping statements reliably:

- **Standalone miners are gradually replaced by the mining pools and cooperative miners.**
- **Transactions related to the mining pool and exchanges take up a large share of the total transactions.**

The latter analysis shows that there are a large number of arbitrage transactions transferring the coins from one exchange to another to make a price difference.