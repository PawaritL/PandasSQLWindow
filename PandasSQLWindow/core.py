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
      Number of rows (up to and including the current row) to consider for rolling functions
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
            raise ValueError("window_rows and window_time cannot both be specified")
        elif (rows_rolling is not None) or (time_rolling is not None):
            self.rolling = True
        else:
            self.rolling = False

        self.partition_by = partition_by
        self.order_by = order_by
        self.ascending = ascending
        self.rows_rolling = None
        self.time_rolling = None

        self.window = data.sort_values(order_by, ascending=ascending).groupby(partition_by)
        self.rolling_window = None
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

    def shift(self, column, **kwargs):
        s = self.window[column].shift(**kwargs)
        return self.postprocess(s)
    def lag(self, column, **kwargs):
        return self.shift(column, **kwargs)
    def lead(self, column, **kwargs):
        kwargs['periods'] = -kwargs['periods'] if kwargs.get('periods') else -1
        return self.shift(column, **kwargs)
    def last(self, column):
        """Finds last previously known non-nan value."""
        s = self.window[column].shift().ffill()
        return self.postprocess(s)
    def rank(self, **kwargs):
        s = self.window[self.order_by].rank(**kwargs).astype(int)
        return self.postprocess(s)
    def count(self, **kwargs):
        kwargs.setdefault('method', 'first')
        return self.rank(**kwargs)

    #-------- Expanding Window Functions --------#
    
    def expanding_min(self, column, **kwargs):
        s = self.window[column].expanding().min(**kwargs)
        return self.postprocess(s, reshape=True)
    def expanding_max(self, column, **kwargs):
        s = self.window[column].expanding().max(**kwargs)
        return self.postprocess(s, reshape=True)
    def expanding_mean(self, column, **kwargs):
        s = self.window[column].expanding().mean(**kwargs)
        return self.postprocess(s, reshape=True)
    def expanding_sum(self, column,  **kwargs):
        s = self.window[column].expanding().sum(**kwargs)
        return self.postprocess(s, reshape=True)
    def expanding_quantile(self, column, **kwargs):
        s = self.window[column].expanding().quantile(**kwargs)
        return self.postprocess(s, reshape=True)
    def expanding_median(self, column, **kwargs):
        kwargs["quantile"] = 0.5
        return self.expanding_quantile(column, **kwargs)
    def expanding_var(self, column, **kwargs):
        s = self.window[column].expanding().var(**kwargs)
        return self.postprocess(s, reshape=True)
    def expanding_std(self, column, **kwargs):
        s = self.window[column].expanding().std(**kwargs)
        return self.postprocess(s, reshape=True)

    #-------- Rolling Window Functions --------#
    
    def check_rolling(self):
        if self.rolling_window is None:
            raise TypeError("To use rolling windows, please specify rows_rolling or time_rolling in Window definition")
    
    def rolling_min(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].min(**kwargs)
        return self.postprocess(s, reshape=True)    
    def rolling_max(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].max(**kwargs)
        return self.postprocess(s, reshape=True)
    def rolling_mean(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].mean(**kwargs)
        return self.postprocess(s, reshape=True)
    def rolling_sum(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].sum(**kwargs)
        return self.postprocess(s, reshape=True)
    def rolling_quantile(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].quantile(**kwargs)
        return self.postprocess(s, reshape=True)
    def rolling_median(self, column, **kwargs):
        self.check_rolling()
        kwargs["quantile"] = 0.5
        return self.rolling_quantile(column, **kwargs)
    def rolling_var(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].var(**kwargs)
        return self.postprocess(s, reshape=True)
    def rolling_std(self, column, **kwargs):
        self.check_rolling()
        s = self.rolling_window[column].std(**kwargs)
        return self.postprocess(s, reshape=True)
    
    #-------- Overload Window Functions --------#   
        
    def min(self, column, **kwargs):
        if self.rolling: return self.rolling_min(column, **kwargs)
        else: return self.expanding_min(column, **kwargs)
    def max(self, column, **kwargs):
        if self.rolling: return self.rolling_max(column, **kwargs)
        else: return self.expanding_max(column, **kwargs)
    def mean(self, column, **kwargs):
        if self.rolling: return self.rolling_mean(column, **kwargs)
        else: return self.expanding_mean(column, **kwargs)
    def sum(self, column, **kwargs):
        if self.rolling: return self.rolling_sum(column, **kwargs)
        else: return self.expanding_sum(column, **kwargs)
    def cumsum(self, column,**kwargs):
        return self.expanding_sum(column, **kwargs)
    def quantile(self, column, **kwargs):
        if self.rolling: return self.rolling_quantile(column, **kwargs)
        else: return self.expanding_quantile(column, **kwargs)
    def median(self, column, **kwargs):
        if self.rolling: return self.rolling_median(column, **kwargs)
        else: return self.expanding_median(column, **kwargs)
    def var(self, column, **kwargs):
        if self.rolling: return self.rolling_var(column, **kwargs)
        else: return self.expanding_var(column, **kwargs)
    def std(self, column, **kwargs):
        if self.rolling: return self.rolling_std(column, **kwargs)
        else: return self.expanding_std(column, **kwargs)
