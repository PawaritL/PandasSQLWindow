class Window:

    """
    SQL Window Functions in a unified, simple Pandas API.
    Follows the ... PARTITION BY ... ORDER BY ... format from SQL.

    Especially helpful for working with data with many logically-partitioned 'groups' 
    or for those more familiar with Window Functions from SQL or Apache Spark.

    Commonly requested functions:
    last() - finds the last previously known non-nan value 
             before the current row, within the same group
    lag() - find the preceding value 
            before the current row, within the same group
    lead() - finds the succeeding value
             after the current row, within the same group

    The current list only serves to demonstrate a few functionalities
    and is by no means exhaustive. Please feel free to reach out with
    any suggestions or requests.

    Parameters
    ----------
    data: Pandas DataFrame
    partition_by: str or list of str
      Name(s) of groupby column(s)
    order_by: str
      Name of sorting column. For rolling functions, this column
      must be datatime-like
    ascending: bool (default=True)
      Sort ascending vs. descending

    rows_rolling: int (default=None)
      Number of rows to consider for rolling functions
      (e.g. rolling_min, rolling_max, rolling_mean)
    time_rolling: offset (default=None)
      Offset time period (e.g. '10s' for 10 seconds)
      to consider for rolling functions
      (e.g. rolling_min, rolling_max, rolling_mean)
    """
    
    def __init__(self, 
                 data, 
                 partition_by, 
                 order_by, 
                 ascending=True,
                 rows_rolling=None,
                 time_rolling=None):
    
        if (rows_rolling is not None) and (time_rolling is not None):
            raise InputError("window_rows and window_time cannot both be specified")

        self.partition_by = partition_by
        self.order_by = order_by
        self.ascending = ascending
        self.rows_rolling = None
        self.time_rolling = None

        self.window = data.sort_values(order_by, ascending=ascending).groupby(partition_by)
        if rows_rolling is not None:
            self.rolling_window = self.window.rolling(rows_rolling, min_periods=1)
        elif time_rolling is not None:
            self.rolling_window = self.window.rolling(time_rolling, min_periods=1)
        return

        @staticmethod
        def postprocess(object, reshape=False, sort_index=True):
            if reshape: shaped = object.reset_index(level=0, drop=True)
            else: shaped = object
            if sort_index: return shaped.sort_index()
            else: return shaped

        def shift(self, column, periods=1):
            s = self.window[column].shift(periods=periods)
            return self.postprocess(s)
        def lag(self, column, periods=1):
            return self.shift(column, periods=periods)
        def lead(self, column, periods=1):
            return self.shift(column, periods=-periods)

        def last(self, column):
            """
            Finds last previously known non-nan value.
            """
            s = self.window[column].shift().ffill()
            return self.postprocess(s)

        def rank(self, method='first'):
            s = self.window[self.order_by].rank(method=method).astype(int)
            return self.postprocess(s)

        def expanding_min(self, column):
            s = self.window[column].expanding().min()
            return self.postprocess(s, reshape=True)
        def expanding_max(self, column):
            s = self.window[column].expanding().max()
            return self.postprocess(s, reshape=True)
        def expanding_mean(self, column):
            s = self.window[column].expanding().mean()
            return self.postprocess(s, reshape=True)
        def expanding_sum(self, column):
            s = self.window[column].expanding().sum()
            return self.postprocess(s, reshape=True)

        def rolling_min(self, column):
            s = self.rolling_window[column].min()
            return self.postprocess(s, reshape=True)    
        def rolling_max(self, column):
            s = self.rolling_window[column].max()
            return self.postprocess(s, reshape=True)
        def rolling_mean(self, column):
            s = self.rolling_window[column].mean()
            return self.postprocess(s, reshape=True)
        def rolling_sum(self, column):
            s = self.rolling_window[column].sum()
            return self.postprocess(s, reshape=True)
