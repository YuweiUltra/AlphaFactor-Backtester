from zipline.pipeline import CustomFactor
import numpy as np

class ScoreFactor(CustomFactor):
    window_length = 1
    inputs = []
    dtype = float
    scores = None

    def compute(self, today, assets, out, *inputs):
        out[:] = ScoreFactor.scores.loc[today].reindex(assets, fill_value=np.nan).values

class IsFilingDateFactor(CustomFactor):
    window_length = 1
    inputs = []
    dtype = float
    scores = None

    def compute(self, today, assets, out, *inputs):
        is_filing_date = ((ScoreFactor.scores != ScoreFactor.scores.shift(1)) & ScoreFactor.scores.notna())
        out[:] = is_filing_date.loc[today].reindex(assets, fill_value=np.nan).values