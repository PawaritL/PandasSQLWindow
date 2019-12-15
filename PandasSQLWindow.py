import pandas as pd

class PandasSQLWindow:

  def __init__(self, 
               data, 
               partition_by, 
               order_by, 
               ascending=True,
               rows_rolling=None,
               time_rolling=None):
    
    """
    SQL-like or Window Functions in a unified Pandas API.
    
    Helpful when working with grouped data and for those more familiar with
    Window functions from SQL or Apache Spark.
    
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

  def shift(self, column, periods=1):
    return self.window[column].shift(periods=periods).sort_index()
  def lag(self, column, periods=1):
    return self.shift(column, periods=periods)
  def lead(self, column, periods=1):
    return self.shift(column, periods=-periods)

  def rank(self, method='first'):
    return self.window[self.order_by].rank(method=method).astype(int).sort_index()
  def cumsum(self, column):
    return self.window[column].cumsum().sort_index()

  def expanding_min(self, column):
    return self.window[column].expanding().min().reset_index(level=0, drop=True).sort_index()
  def expanding_max(self, column):
    return self.window[column].expanding().max().reset_index(level=0, drop=True).sort_index()
  def expanding_mean(self, column):
    return self.window[column].expanding().mean().reset_index(level=0, drop=True).sort_index()
  def expanding_sum(self, column):
    return self.cumsum(column)

  def rolling_min(self, column):
    return self.rolling_window[column].min().reset_index(level=0, drop=True).sort_index()
  def rolling_max(self, column):
    return self.rolling_window[column].max().reset_index(level=0, drop=True).sort_index()
  def rolling_mean(self, column):
    return self.rolling_window[column].mean().reset_index(level=0, drop=True).sort_index()
  def rolling_sum(self, column):
    return self.rolling_window[column].sum().reset_index(level=0, drop=True).sort_index()
