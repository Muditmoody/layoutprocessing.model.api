import pandas as pd
import re
from datetime import datetime


class date_util:

    @staticmethod
    def date(d: str, date_pattern):
        if re.match(date_pattern, d):
            return pd.Timestamp.max.strftime("%d-%m-%Y")
        else:
            return d

    @staticmethod
    def format_lifeline_or_missing_date(dates: pd.Series, date_pattern, date_format, parser_format):
        return dates.fillna(pd.Timestamp.min.strftime(parser_format)) \
            .replace("nan", pd.Timestamp.min.strftime(parser_format)) \
            .replace("", pd.Timestamp.min.strftime(parser_format)) \
            .apply(lambda x: pd.Timestamp.max.strftime(parser_format) if re.match(date_pattern, x) else x) \
            .apply(lambda x: datetime.strptime(x, parser_format).strftime(date_format))
