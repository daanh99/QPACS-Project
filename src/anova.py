import statsmodels.api as sm
from statsmodels.formula.api import ols
import pandas as pd

df = pd.read_csv('replicated_results.csv')
m = ols("throughput ~ C(cores)*C(ram)*C(network)*C(batch_size)*C(nodes)", data=df).fit()
anova = sm.stats.anova_lm(m, type=5)
anova.to_csv('anova.csv')
print(anova)
